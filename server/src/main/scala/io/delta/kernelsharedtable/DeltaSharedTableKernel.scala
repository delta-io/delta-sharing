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

import java.io.FileNotFoundException
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.Timestamp
import java.time.OffsetDateTime
import java.time.format.{DateTimeFormatter, DateTimeParseException}
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
import io.delta.kernel.exceptions.{KernelException, TableNotFoundException}
import io.delta.kernel.internal.{ScanImpl, SnapshotImpl}
import io.delta.kernel.internal.actions.{DeletionVectorDescriptor => KernelDeletionVectorDescriptor, Metadata => KernelMetadata, Protocol => KernelProtocol}
import io.delta.kernel.internal.util.{InternalUtils, VectorUtils}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.azure.NativeAzureFileSystem
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructType}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import io.delta.sharing.server.{AbfsFileSigner, CausedBy, DeltaSharedTableProtocol, DeltaSharingIllegalArgumentException, DeltaSharingUnsupportedOperationException, ErrorStrings, GCSFileSigner, PreSignedUrl, QueryResult, S3FileSigner, WasbFileSigner}
import io.delta.sharing.server.actions.{DeltaFormat, DeltaMetadata, DeltaProtocol, DeltaSingleAction}
import io.delta.sharing.server.config.TableConfig
import io.delta.sharing.server.model._
import io.delta.sharing.server.protocol.{QueryTablePageToken, RefreshToken}
import io.delta.sharing.server.util.JsonUtils

object QueryTypes extends Enumeration {
  type QueryType = Value

  val QueryLatestSnapshot, QueryVersionSnapshot, QueryCDF, QueryStartingVersion = Value
}

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

  /**
   * Run `func` under the classloader of `DeltaSharedTable`. We cannot use the classloader set by
   * Armeria as Hadoop needs to search the classpath to find its classes.
   */
  private def withClassLoader[T](func: => T): T = {
    val classLoader = Thread.currentThread().getContextClassLoader
    if (classLoader == null) {
      Thread.currentThread().setContextClassLoader(this.getClass.getClassLoader)
      try func finally {
        Thread.currentThread().setContextClassLoader(null)
      }
    } else {
      func
    }
  }

  // Get the table and table client (engine).
  // Uses the delta-kernel default implementation of link engine based on Hadoop APIs
  private def getTableAndEngine(): (Table, Engine) = {
    val engine = DefaultEngine.create(
      new Configuration()
    )
    val table = Table.forPath(engine, tablePath.toString)

    (table, engine)
  }

  // Get the table snapshot of the specified version or timestamp
  // TODO: Validate the protocol and metadata
  private def getSharedTableSnapshot(
      table: Table,
      engine: Engine,
      version: Option[Long],
      timestamp: Option[String],
      validStartVersion: Long,
      clientReaderFeatures: Set[String],
      flagReaderFeatures: Set[String] = Set.empty,
      isProviderRpc: Boolean = false): SharedTableSnapshot = {
    val snapshot =
      if (version.isDefined) {
        try {
          table.getSnapshotAsOfVersion(engine, version.get).asInstanceOf[SnapshotImpl]
        } catch {
          case e: io.delta.kernel.exceptions.TableNotFoundException =>
            throw new FileNotFoundException(e.getMessage)
        }

      } else if (timestamp.isDefined) {
        try {
          table
            .getSnapshotAsOfTimestamp(engine, millisSinceEpoch(timestamp.get))
            .asInstanceOf[SnapshotImpl]
        } catch {
          // Convert to DeltaSharingIllegalArgumentException to return 4xx instead of 5xx error code
          // Only convert known exceptions around timestamp too late or too early
          case e: io.delta.kernel.exceptions.TableNotFoundException =>
            throw new FileNotFoundException(e.getMessage)
        }
      } else {
        try {
          table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
        }
        catch {
          case e: io.delta.kernel.exceptions.TableNotFoundException =>
            throw new FileNotFoundException(e.getMessage)
          case e: Throwable =>
            if (e.getMessage.contains("Log file not found")) {
              throw new FileNotFoundException(e.getMessage)
            }
            throw e

        }
      }

    // Validate snapshot version, throw error if it's before the valid start version.
    val snapshotVersion = snapshot.getVersion(engine)

    val protocol = snapshot.getProtocol
    val metadata = snapshot.getMetadata

    val updatedProtocol =
      new KernelProtocol(
        1,
        if (protocol.getMinWriterVersion < 7) {
          protocol.getMinWriterVersion
        } else {
          // minWriterVersion must be < 7 if writerFeatures are empty
          // to bypass delta version check.
          2
        },
        seqAsJavaList(Seq.empty[String]),
        seqAsJavaList(Seq.empty[String])
      )

    // val versionStats =
    //  loadVersionStats(snapshotVersion, clientReaderFeatures, flagReaderFeatures, isProviderRpc)

    SharedTableSnapshot(
      snapshotVersion,
      updatedProtocol,
      metadata,
      // versionStats,
      snapshot
    )
  }

  private lazy val fullHistoryShared: Boolean =
    tableConfig.historyShared && tableConfig.startVersion == 0L
  private lazy val clientReaderFeatures = Set.empty


  /** Get table version at or after startingTimestamp if it's provided, otherwise return
   *  the latest table version.
   */
  override def getTableVersion(startingTimestamp: Option[String]): Long = withClassLoader {
    if (startingTimestamp.isDefined) {
      val (table, engine) = getTableAndEngine()
      try {
        val snapshot = table.
          getSnapshotAsOfTimestamp(engine, millisSinceEpoch(startingTimestamp.get))
        val pastVersion = snapshot.getVersion(engine)

        try {
          // If past snapshot exists, add one to the corresponding version.
          table.getSnapshotAsOfVersion(engine, pastVersion + 1)
          return pastVersion + 1
        } catch {
          // If the snapshot as of this version + 1 does not exist, throw error.
          case _: TableNotFoundException =>
            throw new DeltaSharingIllegalArgumentException("Not a valid starting timestamp")
        }
      } catch {
        // If there was an illegal argument from the timestamp throw the error
        case e: DeltaSharingIllegalArgumentException =>
          throw e
        // If the kernel exception indicates the timestamp was too small, return 0, otherwise error
        case e: KernelException =>
          if (e.getMessage.contains("Please use a timestamp greater than or equal")) {
            return 0
          }
          throw e
      }
    } else {
      // if starting timestamp is not provided use the latest snapshot
      val (table, engine) = getTableAndEngine()
      val snapshot = table.getLatestSnapshot(engine)
      return snapshot.getVersion(engine)
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
      responseFormatSet: Set[String],
      clientReaderFeaturesSet: Set[String] = Set.empty): QueryResult = withClassLoader {

    if (Seq(version, timestamp, startingVersion).filter(_.isDefined).size >= 2) {
      throw new DeltaSharingIllegalArgumentException(
        ErrorStrings.multipleParametersSetErrorMsg(Seq("version", "timestamp", "startingVersion"))
      )
    }

    val (table, engine) = getTableAndEngine()

    val snapshot = getSharedTableSnapshot(
      table,
      engine,
      version,
      timestamp,
      validStartVersion = tableConfig.startVersion,
      clientReaderFeatures = Set.empty
    )

    val respondedFormat = getRespondedFormat(
      responseFormatSet,
      includeFiles,
      snapshot.protocol.getMinReaderVersion
    )

    val protocol = getResponseProtocol(snapshot.protocol, respondedFormat)

    val metadata = getResponseMetadata(snapshot.metadata, version = null, respondedFormat)
    val actions = Seq(
      protocol,
      metadata
    )

    if (includeFiles) {
      throw new DeltaSharingUnsupportedOperationException("not implemented yet")
    }

    QueryResult(snapshot.version, actions, respondedFormat)
  }


  override def queryCDF(
      cdfOptions: Map[String, String],
      includeHistoricalMetadata: Boolean = false,
      maxFiles: Option[Int],
      pageToken: Option[String],
      responseFormatSet: Set[String] = Set("parquet")): QueryResult = {

    throw new DeltaSharingUnsupportedOperationException("not implemented yet")
  }

  protected def getRespondedFormat(
      responseFormatSet: Set[String],
      includeFiles: Boolean,
      minReaderVersion: Int): String = {
    if (includeFiles) {
      if (responseFormatSet.size != 1) {
        // The protocol allows this, but we disallow this in the databricks server reduce the code
        // complexity because there's no actual use case.
        throw new DeltaSharingUnsupportedOperationException(
          s"There can be only 1 responseFormat specified for queryTable or queryTableChanges RPC" +
            s":${responseFormatSet.mkString(",")}."
        )
      }
      responseFormatSet.head
    } else {
      // includeFiles is false, indicate the query is getTableMetadata.
      if (responseFormatSet.size == 1) {
        // Return as the requested format if there's only one format supported.
        responseFormatSet.head
      } else {
        // the client can process both parquet and delta response, the server can decide the
        // respondedFormat
        if (minReaderVersion == 1 &&
          responseFormatSet.contains(DeltaSharedTableKernel.RESPONSE_FORMAT_PARQUET)) {
          // Use parquet as the respondedFormat when 1) it's getTableMetadata rpc 2) the flag is
          // set to true 3) the requested format is delta,parquet 4) the shared table doesn't have
          // advanced delta features.
          DeltaSharedTableKernel.RESPONSE_FORMAT_PARQUET
        } else {
          DeltaSharedTableKernel.RESPONSE_FORMAT_DELTA
        }
      }
    }
  }


  private def getResponseProtocol(
      p: KernelProtocol,
      respondedFormat: String): Object = {
    if (respondedFormat == DeltaSharedTableKernel.RESPONSE_FORMAT_DELTA) {
      val readerFeatures = if (p.getReaderFeatures.isEmpty) {
        None
      } else {
        Some(p.getReaderFeatures.asScala.toSet)
      }
      val writerFeatures = if (p.getWriterFeatures.isEmpty) {
        None
      } else {
        Some(p.getWriterFeatures.asScala.toSet)
      }
      DeltaFormatResponseProtocol(
        deltaProtocol = DeltaProtocol(
          minReaderVersion = p.getMinReaderVersion,
          minWriterVersion = p.getMinWriterVersion,
          readerFeatures = readerFeatures,
          writerFeatures = writerFeatures
        )
      ).wrap
    } else {
      Protocol(p.getMinReaderVersion).wrap
    }
  }

  private def getResponseMetadata(
      m: KernelMetadata,
      version: java.lang.Long,
      respondedFormat: String): Object = {
    // We override the metadata.id to be the table ID instead of the randomly generated ID defined
    // by the Delta Lake protocol. The table ID is a UUID that uniquely identifies a shared table on
    // the delta sharing server.
    if (respondedFormat == DeltaSharedTableKernel.RESPONSE_FORMAT_DELTA) {
      DeltaResponseMetadata(
        deltaMetadata = DeltaMetadataCopy(
          id = m.getId,
          name = m.getName.orElse(null),
          description = m.getDescription.orElse(null),
          format = DeltaFormat(
            provider = m.getFormat.getProvider,
            options = m.getFormat.getOptions.asScala.toMap
          ),
          schemaString = cleanUpTableSchema(m.getSchemaString),
          partitionColumns = getPartitionColumns(m),
          configuration = m.getConfiguration.asScala.toMap,
          createdTime = Option(m.getCreatedTime.orElse(null))
        ),
        version = version
      ).wrap
    } else {
      Metadata(
        id = m.getId,
        name = m.getName.orElse(null),
        description = m.getDescription.orElse(null),
        format = Format(),
        schemaString = cleanUpTableSchema(m.getSchemaString),
        partitionColumns = getPartitionColumns(m),
        configuration = getMetadataConfiguration(tableConf = m.getConfiguration.asScala.toMap),
        version = version
      ).wrap
    }
  }

  // Get the partition columns from Kernel Metadata.
  private def getPartitionColumns(metadata: KernelMetadata): Seq[String] = {
    VectorUtils.toJavaList[String](metadata.getPartitionColumns).asScala
  }

  // Get the table configuration for parquet format response.
  // If the response is in parquet format, return enableChangeDataFeed config only.
  private def getMetadataConfiguration(tableConf: Map[String, String]): Map[String, String ] = {
    if (tableConfig.historyShared &&
      tableConf.getOrElse("delta.enableChangeDataFeed", "false") == "true") {
      Map("enableChangeDataFeed" -> "true")
    } else {
      Map.empty
    }
  }

  private def cleanUpTableSchema(schemaString: String): String = {
    StructType(DataType.fromJson(schemaString).asInstanceOf[StructType].map { field =>
      val newMetadata = new MetadataBuilder()
      // Only keep the column comment
      if (field.metadata.contains("comment")) {
        newMetadata.putString("comment", field.metadata.getString("comment"))
      }
      field.copy(metadata = newMetadata.build())
    }).json
  }

  // Parse timestamp string and return the timestamp in milliseconds since epoch.
  // Accepted format: DateTimeFormatter.ISO_OFFSET_DATE_TIME, example: 2022-01-01T00:00:00Z
  private def millisSinceEpoch(timestamp: String): Long = {
    try {
      OffsetDateTime
        .parse(timestamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        .toInstant
        .toEpochMilli
    } catch {
      case e: DateTimeParseException =>
        try {
          // TODO: stop supporting this format once all clients are migrated.
          InternalUtils.microsSinceEpoch(Timestamp.valueOf(timestamp)) / 1000
        } catch {
          case _: IllegalArgumentException =>
            throw new
                DeltaSharingIllegalArgumentException(s"Invalid startingTimestamp: ${e.getMessage}.")
        }
    }
  }
}

object DeltaSharedTableKernel {
  val RESPONSE_FORMAT_PARQUET = "parquet"
  val RESPONSE_FORMAT_DELTA = "delta"
  private def encodeToken[T <: GeneratedMessage](token: T): String = {
    Base64.getUrlEncoder.encodeToString(token.toByteArray)
  }
  private def decodeToken[T <: GeneratedMessage](tokenStr: String)(
    implicit protoCompanion: GeneratedMessageCompanion[T]): T = {
    protoCompanion.parseFrom(Base64.getUrlDecoder.decode(tokenStr))
  }
}

private case class SharedTableSnapshot(
    version: Long,
    protocol: KernelProtocol,
    metadata: KernelMetadata,
    // versionStats: Option[VersionStats],
    kernelSnapshot: SnapshotImpl)
