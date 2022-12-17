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
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.catalog.{Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** A DataSource V1 for integrating Delta into Spark SQL batch APIs. */
private[sharing] class DeltaSharingDataSource extends RelationProvider with DataSourceRegister {

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    DeltaSharingDataSource.setupFileSystem(sqlContext)
    val path = parameters.getOrElse("path", throw new IllegalArgumentException(
      "'path' is not specified. If you use SQL to create a Delta Sharing table, " +
        "LOCATION must be specified"))

    var cdfOptions: mutable.Map[String, String] = mutable.Map.empty
    val caseInsensitiveParams = new CaseInsensitiveStringMap(parameters.asJava)
    if (DeltaSharingDataSource.isCDFRead(caseInsensitiveParams)) {
      cdfOptions = mutable.Map[String, String](DeltaSharingDataSource.CDF_ENABLED_KEY -> "true")
      if (caseInsensitiveParams.containsKey(DeltaSharingDataSource.CDF_START_VERSION_KEY)) {
        cdfOptions(DeltaSharingDataSource.CDF_START_VERSION_KEY) = caseInsensitiveParams.get(
          DeltaSharingDataSource.CDF_START_VERSION_KEY)
      }
      if (caseInsensitiveParams.containsKey(DeltaSharingDataSource.CDF_START_TIMESTAMP_KEY)) {
        cdfOptions(DeltaSharingDataSource.CDF_START_TIMESTAMP_KEY) = getFormattedTimestamp(
          caseInsensitiveParams.get(DeltaSharingDataSource.CDF_START_TIMESTAMP_KEY))
      }
      if (caseInsensitiveParams.containsKey(DeltaSharingDataSource.CDF_END_VERSION_KEY)) {
        cdfOptions(DeltaSharingDataSource.CDF_END_VERSION_KEY) = caseInsensitiveParams.get(
          DeltaSharingDataSource.CDF_END_VERSION_KEY)
      }
      if (caseInsensitiveParams.containsKey(DeltaSharingDataSource.CDF_END_TIMESTAMP_KEY)) {
        cdfOptions(DeltaSharingDataSource.CDF_END_TIMESTAMP_KEY) = getFormattedTimestamp(
          caseInsensitiveParams.get(DeltaSharingDataSource.CDF_END_TIMESTAMP_KEY))
      }
    }

    var versionAsOf: Option[Long] = None
    if (parameters.get("versionAsOf").isDefined) {
      try {
        versionAsOf = Some(parameters.get("versionAsOf").get.toLong)
      } catch {
        case _: NumberFormatException =>
          throw new IllegalArgumentException("versionAsOf is not a valid number.")
      }
    }
    val deltaLog = RemoteDeltaLog(path)
    deltaLog.createRelation(versionAsOf, cdfOptions = cdfOptions.toMap)
  }

  private def getFormattedTimestamp(str: String): String = {
    val castResult = Cast(
      Literal(str), TimestampType, Option(SQLConf.get.sessionLocalTimeZone)).eval()
    if (castResult == null) {
      throw new IllegalArgumentException(s"The provided timestamp ($str) cannot be converted to a" +
        s" valid timestamp.")
    }
    DateTimeUtils.toJavaTimestamp(castResult.asInstanceOf[java.lang.Long]).toInstant.toString
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
    PreSignedUrlCache.registerIfNeeded(SparkEnv.get)
  }

  // Based on the read options passed it indicates whether the read was a cdf read or not.
  def isCDFRead(options: CaseInsensitiveStringMap): Boolean = {
    options.containsKey(DeltaSharingDataSource.CDF_ENABLED_KEY) &&
      options.get(DeltaSharingDataSource.CDF_ENABLED_KEY) == "true"
  }

  // Constants for cdf parameters
  final val CDF_ENABLED_KEY = "readChangeFeed"

  final val CDF_START_VERSION_KEY = "startingVersion"

  final val CDF_START_TIMESTAMP_KEY = "startingTimestamp"

  final val CDF_END_VERSION_KEY = "endingVersion"

  final val CDF_END_TIMESTAMP_KEY = "endingTimestamp"
}
