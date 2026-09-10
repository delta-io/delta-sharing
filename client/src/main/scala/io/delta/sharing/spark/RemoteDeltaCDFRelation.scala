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

import java.lang.ref.WeakReference

import scala.collection.mutable.ListBuffer

import org.apache.spark.delta.sharing.{CachedTableManager, TableRefreshResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DeltaSharingScanUtils, Row, SparkSession, SQLContext}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

import io.delta.sharing.client.{DeltaSharingClient, DeltaSharingRestClient}
import io.delta.sharing.client.model.{
  AddCDCFile,
  AddFile,
  AddFileForCDF,
  DeltaTableFiles,
  RemoveFile,
  Table => DeltaSharingTable
}
import io.delta.sharing.client.util.ConfUtils
import io.delta.sharing.spark.util.QueryUtils

case class RemoteDeltaCDFRelation(
    spark: SparkSession,
    snapshotToUse: RemoteSnapshot,
    client: DeltaSharingClient,
    table: DeltaSharingTable,
    cdfOptions: Map[String, String]) extends BaseRelation with PrunedFilteredScan {

  private var prefetchedDeltaTableFiles: Option[DeltaTableFiles] = None

  private lazy val deltaTableFiles = prefetchedDeltaTableFiles.getOrElse(
    client.getCDFFiles(table, cdfOptions, false, None))

  private def usePrefetchedFiles(files: DeltaTableFiles): this.type = {
    require(prefetchedDeltaTableFiles.isEmpty, "Prefetched CDF files have already been supplied")
    prefetchedDeltaTableFiles = Some(files)
    this
  }

  private lazy val baseSchema = {
    if (deltaTableFiles.isVersionlessCDF) {
      DeltaTableUtils.toSchema(deltaTableFiles.metadata.schemaString)
    } else {
      snapshotToUse.schema
    }
  }

  private[sharing] lazy val fileIndexParams = {
    val partitionSchemaOverride = if (deltaTableFiles.isVersionlessCDF) {
      Some(new StructType(
        deltaTableFiles.metadata.partitionColumns.map(column => baseSchema(column)).toArray))
    } else {
      None
    }
    new RemoteDeltaFileIndexParams(
      spark,
      snapshotToUse,
      client.getProfileProvider,
      Some(QueryUtils.getQueryParamsHashId(cdfOptions)),
      partitionSchemaOverride = partitionSchemaOverride)
  }

  override lazy val schema: StructType = {
    if (deltaTableFiles.isVersionlessCDF) {
      baseSchema
    } else {
      DeltaTableUtils.addCdcSchema(baseSchema)
    }
  }

  override def sqlContext: SQLContext = spark.sqlContext

  override def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {
    if (deltaTableFiles.isVersionlessCDF) {
      return scanVersionlessFiles(requiredColumns).rdd
    }

    DeltaSharingCDFReader.changesToDF(
      fileIndexParams,
      requiredColumns,
      deltaTableFiles.addFiles,
      deltaTableFiles.cdfFiles,
      deltaTableFiles.removeFiles,
      schema,
      false,
      _ => {
        val d = client.getCDFFiles(table, cdfOptions, false, None)
        TableRefreshResult(
          DeltaSharingCDFReader.getIdToUrl(d.addFiles, d.cdfFiles, d.removeFiles),
          DeltaSharingCDFReader.getMinUrlExpiration(d.addFiles, d.cdfFiles, d.removeFiles),
          None
        )
      },
      System.currentTimeMillis(),
      DeltaSharingCDFReader.getMinUrlExpiration(
        deltaTableFiles.addFiles,
        deltaTableFiles.cdfFiles,
        deltaTableFiles.removeFiles
      )
    ).rdd
  }

  private def scanVersionlessFiles(requiredColumns: Array[String]): DataFrame = {
    val fileIndex = RemoteDeltaBatchFileIndex(fileIndexParams, deltaTableFiles.files)
    val tablePathWithParams =
      if (ConfUtils.sparkParquetIOCacheEnabled(spark.sessionState.conf)) {
        QueryUtils.getTablePathWithIdSuffix(
          fileIndexParams.path.toString, fileIndexParams.queryParamsHashId.get)
      } else {
        fileIndexParams.path.toString
      }

    CachedTableManager.INSTANCE.register(
      tablePathWithParams,
      DeltaSharingCDFReader.getIdToUrl(deltaTableFiles.files),
      Seq(new WeakReference[AnyRef](fileIndex)),
      fileIndexParams.profileProvider,
      _ => {
        val refreshedFiles = client.getCDFFiles(table, cdfOptions, false, None)
        TableRefreshResult(
          DeltaSharingCDFReader.getIdToUrl(refreshedFiles.files),
          DeltaSharingCDFReader.getMinUrlExpiration(refreshedFiles.files),
          None)
      },
      DeltaSharingCDFReader.getMinUrlExpiration(deltaTableFiles.files).getOrElse(
        System.currentTimeMillis() + CachedTableManager.INSTANCE.preSignedUrlExpirationMs),
      None)

    val relation = HadoopFsRelation(
      fileIndex,
      partitionSchema = fileIndex.partitionSchema,
      dataSchema = schema,
      bucketSpec = None,
      snapshotToUse.fileFormat,
      Map.empty)(spark)
    DeltaSharingScanUtils.ofRows(spark, LogicalRelation(relation))
      .select(requiredColumns.map(c => col(DeltaSharingCDFReader.quoteIdentifier(c))): _*)
  }
}

object RemoteDeltaCDFRelation {
  private[sharing] def fromPrefetchedFiles(
      spark: SparkSession,
      snapshotToUse: RemoteSnapshot,
      client: DeltaSharingClient,
      table: DeltaSharingTable,
      cdfOptions: Map[String, String],
      files: DeltaTableFiles): RemoteDeltaCDFRelation = {
    require(files.isVersionlessCDF, "Prefetched CDF files must be versionless")
    require(
      files.respondedFormat == DeltaSharingRestClient.RESPONSE_FORMAT_DELTA,
      "Prefetched versionless CDF files must use Delta format")
    RemoteDeltaCDFRelation(spark, snapshotToUse, client, table, cdfOptions)
      .usePrefetchedFiles(files)
  }
}

object DeltaSharingCDFReader {
  def getIdToUrl(files: Seq[AddFile]): Map[String, String] = {
    files.map(file => file.id -> file.url).toMap
  }

  def getMinUrlExpiration(files: Seq[AddFile]): Option[Long] = {
    val minUrlExpiration = files
      .flatMap(file => Option(file.expirationTimestamp).map(_.longValue()))
      .reduceOption(_ min _)
    if (CachedTableManager.INSTANCE.isValidUrlExpirationTime(minUrlExpiration)) {
      minUrlExpiration
    } else {
      None
    }
  }

  def changesToDF(
      params: RemoteDeltaFileIndexParams,
      requiredColumns: Array[String],
      addFiles: Seq[AddFileForCDF],
      cdfFiles: Seq[AddCDCFile],
      removeFiles: Seq[RemoveFile],
      schema: StructType,
      isStreaming: Boolean,
      refresher: Option[String] => TableRefreshResult,
      lastQueryTableTimestamp: Long,
      expirationTimestamp: Option[Long]
  ): DataFrame = {
    val dfs = ListBuffer[DataFrame]()
    val refs = ListBuffer[WeakReference[AnyRef]]()

    val fileIndex1 = RemoteDeltaCDFAddFileIndex(params, addFiles)
    refs.append(new WeakReference(fileIndex1))
    dfs.append(scanIndex(fileIndex1, schema, isStreaming))

    val fileIndex2 = RemoteDeltaCDCFileIndex(params, cdfFiles)
    refs.append(new WeakReference(fileIndex2))
    dfs.append(scanIndex(fileIndex2, schema, isStreaming))

    val fileIndex3 = RemoteDeltaCDFRemoveFileIndex(params, removeFiles)
    refs.append(new WeakReference(fileIndex3))
    dfs.append(scanIndex(fileIndex3, schema, isStreaming))

    val tablePathWithParams =
      if (ConfUtils.sparkParquetIOCacheEnabled(params.spark.sessionState.conf)) {
        // Ensure different query shapes against the same table have distinct entries
        // in the pre-signed URL cache.
        QueryUtils.getTablePathWithIdSuffix(
          params.path.toString, params.queryParamsHashId.get
        )
      } else {
        params.path.toString
      }

    CachedTableManager.INSTANCE.register(
      tablePathWithParams,
      getIdToUrl(addFiles, cdfFiles, removeFiles),
      refs.toSeq,
      params.profileProvider,
      refresher,
      if (expirationTimestamp.isDefined) {
        expirationTimestamp.get
      } else {
        lastQueryTableTimestamp + CachedTableManager.INSTANCE.preSignedUrlExpirationMs
      },
      None
    )

    dfs.reduce((df1, df2) => df1.unionAll(df2))
      .select(requiredColumns.map(c => col(quoteIdentifier(c))): _*)
  }

  def getIdToUrl(
      addFiles: Seq[AddFileForCDF],
      cdfFiles: Seq[AddCDCFile],
      removeFiles: Seq[RemoveFile]): Map[String, String] = {
    addFiles.map(a => a.id -> a.url).toMap ++
      cdfFiles.map(c => c.id -> c.url).toMap ++
      removeFiles.map(r => r.id -> r.url).toMap
  }

  // Get the minimum url expiration time across all the cdf files returned from the server.
  def getMinUrlExpiration(
      addFiles: Seq[AddFileForCDF],
      cdfFiles: Seq[AddCDCFile],
      removeFiles: Seq[RemoveFile]
  ): Option[Long] = {
    var minUrlExpiration: Option[Long] = None
    addFiles.foreach { a =>
      if (a.expirationTimestamp != null) {
        minUrlExpiration = if (
          minUrlExpiration.isDefined && minUrlExpiration.get < a.expirationTimestamp) {
          minUrlExpiration
        } else {
          Some(a.expirationTimestamp)
        }
      }
    }
    cdfFiles.foreach { c =>
      if (c.expirationTimestamp != null) {
        minUrlExpiration = if (
          minUrlExpiration.isDefined && minUrlExpiration.get < c.expirationTimestamp) {
          minUrlExpiration
        } else {
          Some(c.expirationTimestamp)
        }
      }
    }
    removeFiles.foreach { r =>
      if (r.expirationTimestamp != null) {
        minUrlExpiration = if (
          minUrlExpiration.isDefined && minUrlExpiration.get < r.expirationTimestamp) {
          minUrlExpiration
        } else {
          Some(r.expirationTimestamp)
        }
      }
    }
    if (!CachedTableManager.INSTANCE.isValidUrlExpirationTime(minUrlExpiration)) {
      minUrlExpiration = None
    }
    minUrlExpiration
  }

  private[sharing] def quoteIdentifier(part: String): String = s"`${part.replace("`", "``")}`"

  /**
   * Build a dataframe from the specified file index. We can't use a DataFrame scan directly on the
   * file names because that scan wouldn't include partition columns.
   */
  private def scanIndex(
      fileIndex: RemoteDeltaCDFFileIndexBase,
      schema: StructType,
      isStreaming: Boolean): DataFrame = {
    val relation = HadoopFsRelation(
      fileIndex,
      fileIndex.partitionSchema,
      schema,
      bucketSpec = None,
      fileIndex.params.snapshotAtAnalysis.fileFormat,
      Map.empty)(fileIndex.params.spark)
    val plan = LogicalRelation(relation, isStreaming = isStreaming)
    DeltaSharingScanUtils.ofRows(fileIndex.params.spark, plan)
  }
}
