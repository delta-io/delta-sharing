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

import java.util.Collections

import org.apache.spark.SparkEnv
import org.apache.spark.delta.sharing.PreSignedUrlCache
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.connector.catalog.{Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** A DataSource V1 for integrating Delta into Spark SQL batch APIs. */
private[sharing] class DeltaSharingDataSource extends TableProvider with RelationProvider
  with DataSourceRegister {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = new StructType()

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    // Return a Table with no capabilities so we fall back to the v1 path.
    new Table {
      override def name(): String = s"V1FallbackTable"
      override def schema(): StructType = new StructType()
      override def capabilities(): java.util.Set[TableCapability] = Collections.emptySet()
    }
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    DeltaSharingDataSource.setupFileSystem(sqlContext)
    val path = parameters.getOrElse(
      "path", throw new IllegalArgumentException("'path' is not specified"))
    val deltaLog = RemoteDeltaLog(path)
    deltaLog.createRelation()
  }

  override def shortName: String = "deltaSharing"
}

private[sharing] object DeltaSharingDataSource {

  def setupFileSystem(sqlContext: SQLContext): Unit = {
    // We have put our class name in the `org.apache.hadoop.fs.FileSystem` resource file. However,
    // this file will be loaded only if the class `FileSystem` is loaded. Hence, it won't work when
    // we add the library after starting Spark. Therefore we change the global `hadoopConfiguration`
    // to make sure we set up `DeltaSharingFileSystem` correctly.
    sqlContext.sparkContext.hadoopConfiguration
      .set("fs.delta-sharing.impl", "io.delta.sharing.spark.DeltaSharingFileSystem")
    PreSignedUrlCache.registerIfNeeded(SparkEnv.get)
  }
}
