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
import org.apache.spark.delta.sharing.CachedTableManager
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{
  And,
  Attribute,
  Cast,
  Expression,
  GenericInternalRow,
  Literal,
  SubqueryExpression
}
import org.apache.spark.sql.execution.datasources.{
  FileFormat,
  FileIndex,
  HadoopFsRelation,
  PartitionDirectory
}
import org.apache.spark.sql.types.{DataType, StructType}

import io.delta.sharing.spark.model.{
  AddCDCFile,
  AddFile,
  AddFileForCDF,
  CDFColumnInfo,
  DeltaTableFiles,
  FileAction,
  Metadata,
  Protocol,
  RemoveFile,
  Table => DeltaSharingTable
}

private[sharing] case class RemoteDeltaFileIndexParams(
    val spark: SparkSession,
    val snapshotAtAnalysis: RemoteSnapshot,
    val profileProvider: DeltaSharingProfileProvider) {
  def path: Path = snapshotAtAnalysis.getTablePath
}

// A base class for all file indices for remote delta log.
private[sharing] abstract class RemoteDeltaFileIndexBase(
    val params: RemoteDeltaFileIndexParams) extends FileIndex {
  override def refresh(): Unit = {}

  override def sizeInBytes: Long = params.snapshotAtAnalysis.sizeInBytes

  override def partitionSchema: StructType = params.snapshotAtAnalysis.partitionSchema

  override def rootPaths: Seq[Path] = params.path :: Nil

  protected def toDeltaSharingPath(f: FileAction): Path = {
    DeltaSharingFileSystem.encode(
      params.profileProvider.getCustomTablePath(params.path.toString), f)
  }

  // A helper function to create partition directories from the specified actions.
  protected def makePartitionDirectories(actions: Seq[FileAction]): Seq[PartitionDirectory] = {
    val timeZone = params.spark.sessionState.conf.sessionLocalTimeZone
    actions.groupBy(_.getPartitionValuesInDF()).map {
      case (partitionValues, files) =>
        val rowValues: Array[Any] = partitionSchema.map { p =>
          Cast(Literal(partitionValues(p.name)), p.dataType, Option(timeZone)).eval()
        }.toArray

        val fileStats = files.map { f =>
          new FileStatus(
            /* length */ f.size,
            /* isDir */ false,
            /* blockReplication */ 0,
            /* blockSize */ 1,
            /* modificationTime */ 0,
            toDeltaSharingPath(f))
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
      partitionFilters)
    new Column(rewrittenFilters.reduceLeftOption(And).getOrElse(Literal(true)))
  }
}

// The index for processing files in a delta snapshot.
private[sharing] case class RemoteDeltaSnapshotFileIndex(
    override val params: RemoteDeltaFileIndexParams,
    limitHint: Option[Long]) extends RemoteDeltaFileIndexBase(params) {

  override def inputFiles: Array[String] = {
    params.snapshotAtAnalysis.filesForScan(Nil, None, this)
      .map(f => toDeltaSharingPath(f).toString)
      .toArray
  }

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    makePartitionDirectories(params.snapshotAtAnalysis.filesForScan(
      partitionFilters ++ dataFilters,
      limitHint,
      this
    ))
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
    actions.map(f => toDeltaSharingPath(f).toString).toArray
  }

  private[sharing] def getIdToUrlMap : Map[String, String] = {
    actions.map { action =>
      action.id -> action.url
    }.toMap
  }
}

// The index classes for CDF file types.

private[sharing] case class RemoteDeltaCDFAddFileIndex(
    override val params: RemoteDeltaFileIndexParams,
    deltaTableFiles: DeltaTableFiles)
    extends RemoteDeltaCDFFileIndexBase(
      params,
      deltaTableFiles.addFilesForCdf,
      CDFColumnInfo.getInternalPartitonSchemaForCDFAddRemoveFile) {
  override def listFiles(
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val updatedFiles = deltaTableFiles.addFilesForCdf.map { a =>
      AddFileForCDF(a.url, a.id, a.getPartitionValuesInDF, a.size, a.version, a.timestamp, a.stats)
    }
    val columnFilter = getColumnFilter(partitionFilters)
    val implicits = params.spark.implicits
    import implicits._
    makePartitionDirectories(updatedFiles.toDS().filter(columnFilter).as[AddFileForCDF].collect())
  }
}

private[sharing] case class RemoteDeltaCDCFileIndex(
    override val params: RemoteDeltaFileIndexParams,
    deltaTableFiles: DeltaTableFiles)
    extends RemoteDeltaCDFFileIndexBase(
      params,
      deltaTableFiles.cdfFiles,
      CDFColumnInfo.getInternalPartitonSchemaForCDC) {
  override def listFiles(
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val updatedFiles = deltaTableFiles.cdfFiles.map { c =>
      AddCDCFile(c.url, c.id, c.getPartitionValuesInDF, c.size, c.version, c.timestamp)
    }
    val columnFilter = getColumnFilter(partitionFilters)
    val implicits = params.spark.implicits
    import implicits._
    makePartitionDirectories(updatedFiles.toDS().filter(columnFilter).as[AddCDCFile].collect())
  }
}

private[sharing] case class RemoteDeltaCDFRemoveFileIndex(
    override val params: RemoteDeltaFileIndexParams,
    deltaTableFiles: DeltaTableFiles)
    extends RemoteDeltaCDFFileIndexBase(
      params,
      deltaTableFiles.removeFiles,
      CDFColumnInfo.getInternalPartitonSchemaForCDFAddRemoveFile) {
  override def listFiles(
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val updatedFiles = deltaTableFiles.removeFiles.map { r =>
      RemoveFile(r.url, r.id, r.getPartitionValuesInDF, r.size, r.version, r.timestamp)
    }
    val columnFilter = getColumnFilter(partitionFilters)
    val implicits = params.spark.implicits
    import implicits._
    makePartitionDirectories(updatedFiles.toDS().filter(columnFilter).as[RemoveFile].collect())
  }
}
