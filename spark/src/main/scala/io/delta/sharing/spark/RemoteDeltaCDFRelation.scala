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

import scala.collection.mutable.ListBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

import io.delta.sharing.spark.model.{Table => DeltaSharingTable}

case class RemoteDeltaCDFRelation(
    schema: StructType,
    sqlContext: SQLContext,
    client: DeltaSharingClient,
    table: DeltaSharingTable,
    cdfOptions: Map[String, String]) extends BaseRelation with PrunedFilteredScan {

  override def buildScan(
    requiredColumns: Array[String],
    filters: Array[Filter]): RDD[Row] = {
    val allFiles = client.getCDFFiles(table, cdfOptions)
    // 1. split allFiles to changeFiles/addFiles/RemoveFiles with CDCDataSpec
    // 2. implement dfs.append logic like this
    // 3. Needs file indexes
    val dfs = ListBuffer[DataFrame]()
    dfs.reduce((df1, df2) => df1.unionAll(df2)).rdd
  }
}
