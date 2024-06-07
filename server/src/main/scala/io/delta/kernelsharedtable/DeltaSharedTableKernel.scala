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

package io.delta.kernelsharedtable

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
import com.google.common.hash.Hashing
import io.delta.kernel.Table
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.{ScanImpl, SnapshotImpl}
import io.delta.standalone.DeltaLog
import io.delta.standalone.internal.actions.{AddCDCFile, AddFile, Metadata, Protocol, RemoveFile}
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.util.ConversionUtils
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.azure.NativeAzureFileSystem
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructType}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import io.delta.sharing.server.{AbfsFileSigner, CausedBy, DeltaSharedTableProtocol, DeltaSharingIllegalArgumentException, DeltaSharingUnsupportedOperationException, ErrorStrings, GCSFileSigner, PreSignedUrl, QueryResult, S3FileSigner, WasbFileSigner}
import io.delta.sharing.server.config.TableConfig
import io.delta.sharing.server.protocol.{QueryTablePageToken, RefreshToken}
import io.delta.sharing.server.util.JsonUtils


/**
 * A util class stores all query parameters. Used to compute the checksum in the page token for
 * query validation.
 */
private case class QueryParamChecksum(
                                       version: Option[Long],
                                       timestamp: Option[String],
                                       startingVersion: Option[Long],
                                       startingTimestamp: Option[String],
                                       endingVersion: Option[Long],
                                       endingTimestamp: Option[String],
                                       predicateHints: Seq[String],
                                       jsonPredicateHints: Option[String],
                                       limitHint: Option[Long],
                                       includeHistoricalMetadata: Option[Boolean])


/**
 * A table class that wraps `DeltaLog` to provide the methods used by the server.
 */
class DeltaSharedTableKernel(
                        tableConfig: TableConfig,
                        preSignedUrlTimeoutSeconds: Long,
                        evaluatePredicateHints: Boolean,
                        evaluateJsonPredicateHints: Boolean,
                        evaluateJsonPredicateHintsV2: Boolean,
                        queryTablePageSizeLimit: Int,
                        queryTablePageTokenTtlMs: Int,
                        refreshTokenTtlMs: Int) extends DeltaSharedTableProtocol {

  protected val tablePath: Path = new Path(tableConfig.getLocation)


  // Get the table and table client (engine). If shouldParallelLoadDeltaLog is true,
  // the table client will use a parallel ParquetHandler. Otherwise, it will use the
  // default ParquetHandler.
  private def getTableAndEngine(): (Table, Engine) = {
    val engine = DefaultEngine.create(
      new Configuration()
    )
    val table = Table.forPath(engine, tablePath.toString)

    (table, engine)
  }


  /** Get table version at or after startingTimestamp if it's provided, otherwise return
   *  the latest table version.
   */
  override def getTableVersion(startingTimestamp: Option[String]): Long = {
    if (startingTimestamp.isDefined) {
      throw new DeltaSharingIllegalArgumentException("starting timestamp not defined")
    } else {
      val (table, engine) = getTableAndEngine()
      val snapshot = table.getLatestSnapshot(engine)
      snapshot.getVersion(engine)

    }
  }

  // scalastyle:off argcount
  override def query(
             includeFiles: Boolean,
             predicateHints: Seq[String],
             jsonPredicateHints: Option[String],
             limitHint: Option[Long],
             version: Option[Long],
             timestamp: Option[String],
             startingVersion: Option[Long],
             endingVersion: Option[Long],
             maxFiles: Option[Int],
             pageToken: Option[String],
             includeRefreshToken: Boolean,
             refreshToken: Option[String],
             responseFormatSet: Set[String]): QueryResult = {

    throw new DeltaSharingUnsupportedOperationException("not implemented yet")

  }

  override def queryCDF(
                         cdfOptions: Map[String, String],
                         includeHistoricalMetadata: Boolean = false,
                         maxFiles: Option[Int],
                         pageToken: Option[String],
                         responseFormatSet: Set[String] = Set("parquet")): QueryResult = {

    throw new DeltaSharingUnsupportedOperationException("not implemented yet")

  }

}
