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

import org.apache.spark.delta.sharing.CachedTableManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DeltaSharingScanUtils, Row, SparkSession, SQLContext}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

import io.delta.sharing.spark.model.{
  CDFColumnInfo,
  DeltaTableFiles,
  Metadata,
  Table => DeltaSharingTable
}

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
    val deltaTabelFiles = client.getCDFFiles(table, cdfOptions)
    val metadata = deltaTabelFiles.metadata
    val params = RemoteDeltaFileIndexParams(spark, snapshotToUse)
    val dfs = ListBuffer[DataFrame]()
    val refs = ListBuffer[WeakReference[AnyRef]]()

    // We unconditionally add all types of files.
    // We will get empty data frames for empty ones, which will get combined later.
    val fileIndex1 = RemoteDeltaCDFAddFileIndex(params, deltaTabelFiles)
    refs.append(new WeakReference(fileIndex1))
    dfs.append(scanIndex(fileIndex1, metadata))

    val fileIndex2 = RemoteDeltaCDCFileIndex(params, deltaTabelFiles)
    refs.append(new WeakReference(fileIndex2))
    dfs.append(scanIndex(fileIndex2, metadata))

    val fileIndex3 = RemoteDeltaCDFRemoveFileIndex(params, deltaTabelFiles)
    refs.append(new WeakReference(fileIndex3))
    dfs.append(scanIndex(fileIndex3, metadata))
    CachedTableManager.INSTANCE.register(
      params.path.toString, getIdToUrl(deltaTabelFiles), refs, () => {
        getIdToUrl(client.getCDFFiles(table, cdfOptions))
      })

    dfs.reduce((df1, df2) => df1.unionAll(df2))
      .select(requiredColumns.map(c => col(quoteIdentifier(c))): _*)
      .rdd
  }

  private def quoteIdentifier(part: String): String = s"`${part.replace("`", "``")}`"

  /**
   * Build a dataframe from the specified file index. We can't use a DataFrame scan directly on the
   * file names because that scan wouldn't include partition columns.
   */
  private def scanIndex(fileIndex: RemoteDeltaCDFFileIndexBase, metadata: Metadata): DataFrame = {
    val relation = HadoopFsRelation(
      fileIndex,
      fileIndex.partitionSchema,
      DeltaTableUtils.addCdcSchema(metadata.schemaString),
      bucketSpec = None,
      snapshotToUse.fileFormat,
      Map.empty)(spark)
    val plan = LogicalRelation(relation)
    DeltaSharingScanUtils.ofRows(spark, plan)
  }

  private def getIdToUrl(deltaTabelFiles: DeltaTableFiles): Map[String, String] = {
    deltaTabelFiles.addFilesForCdf.map(a => a.id -> a.url).toMap ++
      deltaTabelFiles.cdfFiles.map(c => c.id -> c.url).toMap ++
      deltaTabelFiles.removeFiles.map(r => r.id -> r.url).toMap
  }
}
