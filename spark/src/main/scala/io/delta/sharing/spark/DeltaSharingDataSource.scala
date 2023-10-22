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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.SparkEnv
import org.apache.spark.delta.sharing.PreSignedUrlCache
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{
  BaseRelation,
  DataSourceRegister,
  RelationProvider,
  StreamSourceProvider
}
import org.apache.spark.sql.types.StructType


/** A DataSource V1 for integrating Delta into Spark SQL batch APIs. */
private[sharing] class DeltaSharingDataSource
  extends RelationProvider
    with StreamSourceProvider
    with DataSourceRegister {

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    DeltaSharingDataSource.setupFileSystem(sqlContext)
    val options = new DeltaSharingOptions(parameters)
    val path = options.options.getOrElse("path", throw DeltaSharingErrors.pathNotSpecifiedException)

    val deltaLog = RemoteDeltaLog(
      path, forStreaming = false, responseFormat = options.responseFormat
    )
    deltaLog.createRelation(options.versionAsOf, options.timestampAsOf, options.cdfOptions)
  }

  // Returns the schema of the latest table snapshot.
  override def sourceSchema(
    sqlContext: SQLContext,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]): (String, StructType) = {
    if (schema.nonEmpty && schema.get.nonEmpty) {
      throw DeltaSharingErrors.specifySchemaAtReadTimeException
    }
    val options = new DeltaSharingOptions(parameters)
    if (options.isTimeTravel) {
      throw DeltaSharingErrors.timeTravelNotSupportedException
    }

    val path = options.options.getOrElse("path", throw DeltaSharingErrors.pathNotSpecifiedException)
    val deltaLog = RemoteDeltaLog(
      path, forStreaming = true, responseFormat = options.responseFormat
    )
    val schemaToUse = deltaLog.snapshot().schema
    if (schemaToUse.isEmpty) {
      throw DeltaSharingErrors.schemaNotSetException
    }

    if (options.readChangeFeed) {
      (shortName(), DeltaTableUtils.addCdcSchema(schemaToUse))
    } else {
      (shortName(), schemaToUse)
    }
  }

  override def createSource(
    sqlContext: SQLContext,
    metadataPath: String,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]): Source = {
    DeltaSharingDataSource.setupFileSystem(sqlContext)
    if (schema.nonEmpty && schema.get.nonEmpty) {
      throw DeltaSharingErrors.specifySchemaAtReadTimeException
    }
    val options = new DeltaSharingOptions(parameters)
    val path = options.options.getOrElse("path", throw DeltaSharingErrors.pathNotSpecifiedException)
    val deltaLog = RemoteDeltaLog(path, forStreaming = true, options.responseFormat)

    DeltaSharingSource(SparkSession.active, deltaLog, options)
  }

  override def shortName(): String = "deltaSharing"
}

private[sharing] object DeltaSharingDataSource {
  def setupFileSystem(sqlContext: SQLContext): Unit = {
    // We have put our class name in the `org.apache.hadoop.fs.FileSystem` resource file. However,
    // this file will be loaded only if the class `FileSystem` is loaded. Hence, it won't work when
    // we add the library after starting Spark. Therefore we change the global `hadoopConfiguration`
    // to make sure we set up `DeltaSharingFileSystem` correctly.
    sqlContext.sparkContext.hadoopConfiguration
      .setIfUnset("fs.delta-sharing.impl", "io.delta.sharing.client.DeltaSharingFileSystem")
    PreSignedUrlCache.registerIfNeeded(SparkEnv.get)
  }
}
