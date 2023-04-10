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
import org.apache.spark.sql.connector.catalog.{Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.{
  ColumnWithDefaultExprUtils,
  DeltaErrors,
  DeltaLog,
  DeltaOptions
}
import org.apache.spark.sql.delta.DeltaTableUtils.extractIfPathContainsTimeTravel
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSource}
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{
  BaseRelation,
  DataSourceRegister,
  RelationProvider,
  StreamSourceProvider
}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

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

    // scalastyle:off println
    Console.println(s"----[linzhou]----createRelation for path: $path")
    if (path.startsWith("delta-sharing-log")) {
      Console.println(s"----[linzhou]----createRelation using DeltaLog, parameters: $parameters")
      val dL = DeltaLog.forTable(sqlContext.sparkSession, path, parameters)
      return dL.createRelation(cdcOptions = new CaseInsensitiveStringMap(options.cdfOptions.asJava))
    }
    Console.println(s"----[linzhou]---- createRelation using RemoteDeltaLog")
    val deltaLog = RemoteDeltaLog(path)
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
    Console.println(s"----[linzhou]----sourceSchema for path: $path")
    if (path.startsWith("delta-sharing-log")) {
      Console.println(s"----[linzhou]----sourceSchema using DeltaLog")
      val (_, maybeTimeTravel) = extractIfPathContainsTimeTravel(
        sqlContext.sparkSession, path, Map.empty)
      if (maybeTimeTravel.isDefined) throw DeltaErrors.timeTravelNotSupportedException
      if (DeltaDataSource.getTimeTravelVersion(parameters).isDefined) {
        throw DeltaErrors.timeTravelNotSupportedException
      }

      val (_, snapshot) = DeltaLog.forTableWithSnapshot(sqlContext.sparkSession, path)
      val readSchema = snapshot.schema

      val schemaToUse = ColumnWithDefaultExprUtils.removeDefaultExpressions(readSchema)
      if (schemaToUse.isEmpty) {
        throw DeltaErrors.schemaNotSetException
      }
      val optionsDelta = new CaseInsensitiveStringMap(parameters.asJava)
      Console.println(s"----[linzhou]----schemaToUse: $schemaToUse")
      if (CDCReader.isCDCRead(optionsDelta)) {
        return (shortName(), CDCReader.cdcReadSchema(schemaToUse))
      } else {
        return (shortName(), schemaToUse)
      }
    }

    val deltaLog = RemoteDeltaLog(path, forStreaming = true)
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

    Console.println(s"----[linzhou]----createSource: for path $path")
    if (path.startsWith("delta-sharing-log")) {
      Console.println(s"----[linzhou]----createSource using DeltaLog")
      val options2 = new DeltaOptions(parameters, sqlContext.sparkSession.sessionState.conf)
      val (deltaLog, snapshot) = DeltaLog.forTableWithSnapshot(sqlContext.sparkSession, path)
      val readSchema = snapshot.schema

      if (readSchema.isEmpty) {
        throw DeltaErrors.schemaNotSetException
      }
      return DeltaSharingDeltaSource(
        DeltaSource(
          sqlContext.sparkSession,
          deltaLog,
          options2,
          snapshot
        )
      )
    }

    val deltaLog = RemoteDeltaLog(path, forStreaming = true)

    DeltaSharingSource(SparkSession.active, deltaLog, options)
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
      .setIfUnset("fs.delta-sharing.impl", "io.delta.sharing.spark.DeltaSharingFileSystem")
    sqlContext.sparkContext.hadoopConfiguration
      .setIfUnset("fs.delta-sharing-log.impl", "io.delta.sharing.spark.DeltaSharingLogFileSystem")
    PreSignedUrlCache.registerIfNeeded(SparkEnv.get)
  }
}
