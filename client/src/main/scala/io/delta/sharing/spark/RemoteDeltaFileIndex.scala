/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.sharing.spark

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Cast, Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitionDirectory}
import org.apache.spark.sql.types.{DataType, StructType}

import io.delta.sharing.client.{DeltaSharingFileSystem, DeltaSharingProfileProvider}
import io.delta.sharing.client.model.{
  AddCDCFile,
  AddFile,
  AddFileForCDF,
  CDFColumnInfo,
  FileAction,
  RemoveFile
}
import io.delta.sharing.client.util.{ConfUtils, JsonUtils}
import io.delta.sharing.filters.{AndOp, BaseOp, OpConverter}
import io.delta.sharing.spark.util.QueryUtils

/*
 * The `queryParamsHashId` is used to distinguish queries that share the same table path.
 * For streaming and CDF queries, all file actions are retrieved and cached from the server,
 * so `queryParamsHashId` only includes the start and end versions.
 * For snapshot queries, different Parquet files are retrieved and cached based on filters,
 * resulting in unique `queryParamsHashId` values for each `FileIndex.listFiles` call.
 * Therefore, `queryParamsHashId` here is `Some()` for streaming and CDF queries, and `None`
 * for snapshot queries. For snapshot queries, the `queryParamsHashId` is constructed during
 * `listFiles` calls.
 */
private[sharing] case class RemoteDeltaFileIndexParams(
    spark: SparkSession,
    snapshotAtAnalysis: RemoteSnapshot,
    profileProvider: DeltaSharingProfileProvider,
    queryParamsHashId: Option[String] = None) {
  def path: Path = snapshotAtAnalysis.getTablePath
}

// A base class for all file indices for remote delta log.
private[sharing] abstract class RemoteDeltaFileIndexBase(
    val params: RemoteDeltaFileIndexParams) extends FileIndex with Logging {
  override def refresh(): Unit = {}

  override def sizeInBytes: Long = params.snapshotAtAnalysis.sizeInBytes

  override def partitionSchema: StructType = params.snapshotAtAnalysis.partitionSchema

  override def rootPaths: Seq[Path] = params.path :: Nil

  // Builds a Delta Sharing path for a file action. The result includes both the table path and
  // file ID, which are used to retrieve the file URL from the pre-signed URL cache.
  protected def toDeltaSharingPath(f: FileAction, queryParamsHashId: String): Path = {
    val tablePathWithParams =
      if (ConfUtils.sparkParquetIOCacheEnabled(params.spark.sessionState.conf)) {
        // Adding `queryParamHashId` to the path ensures unique entries in the pre-signed URL cache
        // for different query shapes, distinguishing queries sharing the same table path.
        QueryUtils.getTablePathWithIdSuffix(
          params.profileProvider.getCustomTablePath(params.path.toString),
          queryParamsHashId
        )
      } else {
        params.profileProvider.getCustomTablePath(params.path.toString)
      }

    DeltaSharingFileSystem.encode(tablePathWithParams, f)
  }

  // A helper function to create partition directories from the specified actions.
  protected def makePartitionDirectories(
    actions: Seq[FileAction],
    queryParamsHashId: String): Seq[PartitionDirectory] = {
    val timeZone = params.spark.sessionState.conf.sessionLocalTimeZone
    // The getPartitionValuesInDF function is idempotent, and calling it multiple times does not
    // change its output.
    actions.groupBy(_.getPartitionValuesInDF()).map {
      case (partitionValues, files) =>
        val rowValues: Array[Any] = partitionSchema.map { p =>
          new Cast(Literal(partitionValues(p.name)), p.dataType, Option(timeZone)).eval()
        }.toArray

        val fileStats = files.map { f =>
          new FileStatus(
            /* length */ f.size,
            /* isDir */ false,
            /* blockReplication */ 0,
            /* blockSize */ 1,
            /* modificationTime */ 0,
            toDeltaSharingPath(f, queryParamsHashId))
        }.toArray

        try {
          // Databricks Runtime has a different `PartitionDirectory.apply` method. We need to use
          // Java Reflection to call it.
          classOf[PartitionDirectory].getMethod("apply", classOf[InternalRow], fileStats.getClass)
            .invoke(null, new GenericInternalRow(rowValues), fileStats)
            .asInstanceOf[PartitionDirectory]
        } catch {
          case _: NoSuchMethodException =>
            // This is not in Databricks Runtime. We can call Spark's PartitionDirectory directly.
            PartitionDirectory(new GenericInternalRow(rowValues), fileStats)
        }
    }.toSeq
  }

  protected def getColumnFilter(partitionFilters: Seq[Expression]): Column = {
    val rewrittenFilters = DeltaTableUtils.rewritePartitionFilters(
      params.snapshotAtAnalysis.partitionSchema,
      params.spark.sessionState.conf.resolver,
      partitionFilters,
      params.spark.sessionState.conf.sessionLocalTimeZone)
    new Column(rewrittenFilters.reduceLeftOption(And).getOrElse(Literal(true)))
  }

  // Converts the specified SQL expressions to a json predicate.
  //
  // If jsonPredicatesV2 are enabled, converts both partition and data filters
  // and combines them using an AND.
  //
  // If the conversion fails, returns a None, which will imply that we will
  // not perform json predicate based filtering.
  protected def convertToJsonPredicate(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression] = Seq.empty) : Option[String] = {
    if (!ConfUtils.jsonPredicatesEnabled(params.spark.sessionState.conf)) {
      return None
    }

    // Convert the partition filters.
    val partitionOp = try {
      OpConverter.convert(partitionFilters)
    } catch {
      case e: Exception =>
        log.error("Error while converting partition filters: " + e)
        None
    }

    // If V2 predicates are enabled, also convert the data filters.
    val dataOp = try {
      if (ConfUtils.jsonPredicatesV2Enabled(params.spark.sessionState.conf)) {
        log.info("Converting data filters")
        OpConverter.convert(dataFilters)
      } else {
        None
      }
    } catch {
      case e: Exception =>
        log.error("Error while converting data filters: " + e)
        None
    }

    // Combine partition and data filters using an AND operation.
    val combinedOp = if (partitionOp.isDefined && dataOp.isDefined) {
      Some(AndOp(Seq(partitionOp.get, dataOp.get)))
    } else if (partitionOp.isDefined) {
      partitionOp
    } else {
      dataOp
    }
    log.info("Using combined predicate: " + combinedOp)

    if (combinedOp.isDefined) {
      Some(JsonUtils.toJson[BaseOp](combinedOp.get))
    } else {
      None
    }
  }
}

// The index for processing files in a delta snapshot.
private[sharing] case class RemoteDeltaSnapshotFileIndex(
    override val params: RemoteDeltaFileIndexParams,
    limitHint: Option[Long]) extends RemoteDeltaFileIndexBase(params) {

  // Retrieves and caches all files for a version of the table.
  override def inputFiles: Array[String] = {
    val queryParamsHashId = QueryUtils.getQueryParamsHashId(
      version = params.snapshotAtAnalysis.version
    )
    params.snapshotAtAnalysis.filesForScan(Nil, None, None, this, queryParamsHashId)
      .map(f => toDeltaSharingPath(f, queryParamsHashId).toString)
      .toArray
  }

  // Retrieves and caches files that match the partition filters and limit.
  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val jsonPredicateHints = convertToJsonPredicate(partitionFilters, dataFilters)
    val queryParamsHashId = QueryUtils.getQueryParamsHashId(
      partitionFilters.map(_.sql).mkString(";"),
      dataFilters.map(_.sql).mkString(";"),
      jsonPredicateHints.getOrElse(""),
      limitHint.map(_.toString).getOrElse(""),
      params.snapshotAtAnalysis.version
    )

    makePartitionDirectories(
      params.snapshotAtAnalysis.filesForScan(
        partitionFilters ++ dataFilters,
        limitHint,
        jsonPredicateHints,
        this,
        queryParamsHashId
      ),
      queryParamsHashId
    )
  }
}

// A base class for all file indices for CDF.
private[sharing] abstract class RemoteDeltaCDFFileIndexBase(
    override val params: RemoteDeltaFileIndexParams,
    actions: Seq[FileAction],
    auxPartitionSchema: Map[String, DataType] = Map.empty)
    extends RemoteDeltaFileIndexBase(params) {

  override def partitionSchema: StructType = {
    DeltaTableUtils.updateSchema(params.snapshotAtAnalysis.partitionSchema, auxPartitionSchema)
  }

  override def inputFiles: Array[String] = {
    actions.map(f => toDeltaSharingPath(f, params.queryParamsHashId.getOrElse("")).toString).toArray
  }
}

// The index classes for CDF file types.

private[sharing] case class RemoteDeltaCDFAddFileIndex(
    override val params: RemoteDeltaFileIndexParams,
    addFiles: Seq[AddFileForCDF])
    extends RemoteDeltaCDFFileIndexBase(
      params,
      addFiles,
      CDFColumnInfo.getInternalPartitonSchemaForCDFAddRemoveFile) {
  override def listFiles(
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    // Need to apply getPartitionValuesInDF to each file, to be consistent with
    // makePartitionDirectories and partitionSchema. So that partitionFilters can be correctly
    // applied.
    val updatedFiles = addFiles.map { a =>
      AddFileForCDF(a.url, a.id, a.getPartitionValuesInDF, a.size, a.version, a.timestamp, a.stats,
        a.expirationTimestamp)
    }
    val columnFilter = getColumnFilter(partitionFilters)
    val implicits = params.spark.implicits
    import implicits._
    makePartitionDirectories(
      updatedFiles.toDS().filter(columnFilter).as[AddFileForCDF].collect(),
      params.queryParamsHashId.getOrElse("")
    )
  }
}

private[sharing] case class RemoteDeltaCDCFileIndex(
    override val params: RemoteDeltaFileIndexParams,
    cdfFiles: Seq[AddCDCFile])
    extends RemoteDeltaCDFFileIndexBase(
      params,
      cdfFiles,
      CDFColumnInfo.getInternalPartitonSchemaForCDC) {

  override def listFiles(
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    // Need to apply getPartitionValuesInDF to each file, to be consistent with
    // makePartitionDirectories and partitionSchema. So that partitionFilters can be correctly
    // applied.
    val updatedFiles = cdfFiles.map { c =>
      AddCDCFile(c.url, c.id, c.getPartitionValuesInDF, c.size, c.version, c.timestamp,
        c.expirationTimestamp)
    }
    val columnFilter = getColumnFilter(partitionFilters)
    val implicits = params.spark.implicits
    import implicits._
    makePartitionDirectories(
      updatedFiles.toDS().filter(columnFilter).as[AddCDCFile].collect(),
      params.queryParamsHashId.getOrElse("")
    )
  }
}

private[sharing] case class RemoteDeltaCDFRemoveFileIndex(
    override val params: RemoteDeltaFileIndexParams,
    removeFiles: Seq[RemoveFile])
    extends RemoteDeltaCDFFileIndexBase(
      params,
      removeFiles,
      CDFColumnInfo.getInternalPartitonSchemaForCDFAddRemoveFile) {
  override def listFiles(
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    // Need to apply getPartitionValuesInDF to each file, to be consistent with
    // makePartitionDirectories and partitionSchema. So that partitionFilters can be correctly
    // applied.
    val updatedFiles = removeFiles.map { r =>
      RemoveFile(r.url, r.id, r.getPartitionValuesInDF, r.size, r.version, r.timestamp,
        r.expirationTimestamp)
    }
    val columnFilter = getColumnFilter(partitionFilters)
    val implicits = params.spark.implicits
    import implicits._
    makePartitionDirectories(
      updatedFiles.toDS().filter(columnFilter).as[RemoveFile].collect(),
      params.queryParamsHashId.getOrElse("")
    )
  }
}

// The index classes for batch files
private[sharing] case class RemoteDeltaBatchFileIndex(
  override val params: RemoteDeltaFileIndexParams,
  val addFiles: Seq[AddFile]) extends RemoteDeltaFileIndexBase(params) {

  override def sizeInBytes: Long = {
    addFiles.map(_.size).sum
  }

  override def inputFiles: Array[String] = {
    addFiles.map(a =>
      toDeltaSharingPath(a, params.queryParamsHashId.getOrElse("")).toString).toArray
  }

  override def listFiles(
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val columnFilter = getColumnFilter(partitionFilters)
    val implicits = params.spark.implicits
    import implicits._
    makePartitionDirectories(
      addFiles.toDS().filter(columnFilter).as[AddFile].collect(),
      params.queryParamsHashId.getOrElse("")
    )
  }
}
