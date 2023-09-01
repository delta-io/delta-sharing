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
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

import io.delta.sharing.client.DeltaSharingClient
import io.delta.sharing.client.model.{AddCDCFile, AddFileForCDF, RemoveFile, Table => DeltaSharingTable}

case class RemoteDeltaCDFRelation(
    spark: SparkSession,
    snapshotToUse: RemoteSnapshot,
    client: DeltaSharingClient,
    table: DeltaSharingTable,
    cdfOptions: Map[String, String]) extends BaseRelation with PrunedFilteredScan {

  override def schema: StructType = DeltaTableUtils.addCdcSchema(snapshotToUse.schema)

  override def sqlContext: SQLContext = spark.sqlContext

  override def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {
    val deltaTabelFiles = client.getCDFFiles(table, cdfOptions, false)

    DeltaSharingCDFReader.changesToDF(
      new RemoteDeltaFileIndexParams(spark, snapshotToUse, client.getProfileProvider),
      requiredColumns,
      deltaTabelFiles.addFiles,
      deltaTabelFiles.cdfFiles,
      deltaTabelFiles.removeFiles,
      DeltaTableUtils.addCdcSchema(deltaTabelFiles.metadata.schemaString),
      false,
      _ => {
        val d = client.getCDFFiles(table, cdfOptions, false)
        TableRefreshResult(
          DeltaSharingCDFReader.getIdToUrl(d.addFiles, d.cdfFiles, d.removeFiles),
          DeltaSharingCDFReader.getMinUrlExpiration(d.addFiles, d.cdfFiles, d.removeFiles),
          None
        )
      },
      System.currentTimeMillis(),
      DeltaSharingCDFReader.getMinUrlExpiration(
        deltaTabelFiles.addFiles,
        deltaTabelFiles.cdfFiles,
        deltaTabelFiles.removeFiles
      )
    ).rdd
  }
}

object DeltaSharingCDFReader {
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

    CachedTableManager.INSTANCE.register(
      params.path.toString,
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

  private def quoteIdentifier(part: String): String = s"`${part.replace("`", "``")}`"

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
