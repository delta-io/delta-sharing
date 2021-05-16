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

import scala.collection.JavaConverters._
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.connector.catalog.{Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DeltaSharingDataSource extends TableProvider with RelationProvider with DataSourceRegister {

  DeltaSharingDataSource.reloadFileSystemsIfNeeded()

  // TODO: I have no idea what's going on here. Implementing the DataSourceV2 TableProvider without
  // retaining RelationProvider doesn't work when creating a metastore table; Spark insists on
  // looking up the USING `format` as a V1 source, and will fail if this source only uses v2.
  // But the DSv2 methods are never actually called in the metastore path! What having the V2
  // implementation does do is change the value of parameters passed to the V1 createRelation()
  // method (!!) to include the TBLPROPERTIES we need. (When reading from a file path, though,
  // the v2 path is used as normal.)
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = new StructType()
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    // Return a Table with no capabilities so we fall back to the v1 path.
    new Table {
      override def name(): String = s"V1FallbackTable"

      override def schema(): StructType = new StructType()

      override def capabilities(): java.util.Set[TableCapability] =
        Set.empty[TableCapability].asJava
    }
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse(
      "path", throw new IllegalArgumentException("'path' is not specified"))
    val deltaLog = RemoteDeltaLog(path)
    deltaLog.createRelation()
  }

  override def shortName: String = "deltaSharing"
}

object DeltaSharingDataSource {
  private var reloaded = false

  def reloadFileSystemsIfNeeded(): Unit = synchronized {
    if (!reloaded) {
      reloaded = true
      SparkSession.active.sparkContext.hadoopConfiguration
        .set("fs.delta-sharing.impl", "io.delta.sharing.spark.DeltaSharingFileSystem")
    }
  }
}
