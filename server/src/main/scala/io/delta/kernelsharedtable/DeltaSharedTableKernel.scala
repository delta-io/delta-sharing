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
import io.delta.kernel.data.{ColumnarBatch, ColumnVector, FilteredColumnarBatch}
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.{KernelException, TableNotFoundException}
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.internal.{ScanImpl, SnapshotImpl}
import io.delta.kernel.internal.actions.{DeletionVectorDescriptor => KernelDeletionVectorDescriptor, Metadata => KernelMetadata, Protocol => KernelProtocol}
import io.delta.kernel.internal.util.{InternalUtils, VectorUtils}
import io.delta.kernel.types.{StructType => KernelStructType}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.azure.NativeAzureFileSystem
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructType}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import io.delta.sharing.server.{CausedBy, DeltaSharedTableProtocol, DeltaSharingIllegalArgumentException, DeltaSharingUnsupportedOperationException, ErrorStrings, QueryResult}
import io.delta.sharing.server.common.{AbfsFileSigner, GCSFileSigner, PreSignedUrl, S3FileSigner, SnapshotChecker, WasbFileSigner}
import io.delta.sharing.server.common.actions.{DeletionVectorDescriptor, DeletionVectorsTableFeature, DeltaAddFile, DeltaFormat, DeltaMetadata, DeltaProtocol, DeltaSingleAction}
import io.delta.sharing.server.config.TableConfig
import io.delta.sharing.server.model._
import io.delta.sharing.server.protocol.{QueryTablePageToken, RefreshToken}
import io.delta.sharing.server.util.JsonUtils

object QueryTypes extends Enumeration {
  type QueryType = Value

  val QueryLatestSnapshot, QueryVersionSnapshot, QueryCDF, QueryStartingVersion = Value
}

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

  // The ordinals of the fields in the Kernel scan file, used to extract ColumnVectors
  // from ColumnarBatch.
  private var scanFileFieldOrdinals: ScanFileFieldOrdinals = _

  private var numRecords = 0L
  private var earlyTermination = false
  private var minUrlExpirationTimestamp = Long.MaxValue
  private val JsonPredicateHintSizeLimit = 1L * 1024L * 1024L // 1MB
  private val JsonPredicateHintMaxTreeDepth = 100

  private val fileSigner = withClassLoader {
    val tablePath = new Path(tableConfig.getLocation)
    val conf = new Configuration()
    val fs = tablePath.getFileSystem(conf)
    val (table, engine) = getTableAndEngine()

    val dataPath = new URI(table.getPath(engine))

    fs match {
      case _: S3AFileSystem =>
        new S3FileSigner(dataPath, conf, preSignedUrlTimeoutSeconds)
      case wasb: NativeAzureFileSystem =>
        WasbFileSigner(wasb, dataPath, conf, preSignedUrlTimeoutSeconds)
      case abfs: AzureBlobFileSystem =>
        AbfsFileSigner(abfs, dataPath, preSignedUrlTimeoutSeconds)
      case gc: GoogleHadoopFileSystem =>
        new GCSFileSigner(dataPath, conf, preSignedUrlTimeoutSeconds)
      case _ =>
        throw new IllegalStateException(s"File system ${fs.getClass} is not supported")
    }
  }

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
  private def getSharedTableSnapshot(
      table: Table,
      engine: Engine,
      version: Option[Long],
      timestamp: Option[String],
      validStartVersion: Long,
      clientReaderFeatures: Set[String]): SharedTableSnapshot = {
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

    SnapshotChecker.assertTableProperties(
      metadata.getConfiguration.asScala.toMap,
      Some(snapshotVersion),
      clientReaderFeatures
    )

    val updatedProtocol = if (clientReaderFeatures.isEmpty) {
      // Client does not support advanced features.
      // And after all the validation the table can be treated as having no advanced
      // features/properties, hence we return a "fake" minReaderVersion=1, and cleanup
      // the readerFeatures.
      // Otherwise, we return the original protocol.
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
    } else {
      protocol
    }

    SharedTableSnapshot(
      snapshotVersion,
      updatedProtocol,
      metadata,
      snapshot
    )
  }

  private lazy val fullHistoryShared: Boolean =
    tableConfig.historyShared && tableConfig.startVersion == 0L

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

    refreshToken.map(decodeAndValidateRefreshToken)
    val isVersionQuery = !Seq(version, timestamp).filter(_.isDefined).isEmpty

    val queryType = if (version.isDefined || timestamp.isDefined) {
      QueryTypes.QueryVersionSnapshot
    } else {
      QueryTypes.QueryLatestSnapshot
    }

    val globalLimitHint = if (predicateHints.isEmpty && jsonPredicateHints.isEmpty) {
      limitHint
    } else {
      None
    }

    val (table, engine) = getTableAndEngine()

    val snapshot = getSharedTableSnapshot(
      table,
      engine,
      version,
      timestamp,
      validStartVersion = tableConfig.startVersion,
      clientReaderFeatures = clientReaderFeaturesSet
    )

    val respondedFormat = getRespondedFormat(
      responseFormatSet,
      snapshot.protocol.getMinReaderVersion
    )

    val protocol = getResponseProtocol(snapshot.protocol, respondedFormat)

    val metadata = getResponseMetadata(snapshot.metadata, version = null, respondedFormat)
    var actions = Seq(
      protocol,
      metadata
    )

    // Note that this will not work if
    // predicateHints, maxFiles, startingVersion, and pageToken are not all empty
    if (includeFiles) {
      val predicateOpt = maybeCreatePredicate(
        jsonPredicateHints,
        snapshot.kernelSnapshot.getSchema(engine),
        getPartitionColumns(snapshot.metadata)
      )
      val scanBuilder = snapshot.kernelSnapshot.getScanBuilder(engine)
      val scan = predicateOpt match {
        case Some(p) => scanBuilder.withFilter(engine, p).build()
        case None => scanBuilder.build()
      }

      val scanFilesIter = scan.asInstanceOf[ScanImpl].getScanFiles(engine, true)
      try {
        while (scanFilesIter.hasNext && !earlyTermination) {
          val nextResponse = processBatchByColumnVector(
            scanFilesIter.next,
            snapshot.version,
            queryType,
            respondedFormat,
            table,
            engine,
            isVersionQuery,
            snapshot,
            globalLimitHint,
            clientReaderFeaturesSet
          )
          if (nextResponse.nonEmpty) {
            actions = actions ++ nextResponse
          }
        }
      } finally {
        scanFilesIter.close()
      }

      val refreshTokenStr = if (includeRefreshToken) {
        DeltaSharedTableKernel.encodeToken(
          RefreshToken(
            id = Some(tableConfig.id),
            version = Some(snapshot.version),
            expirationTimestamp = Some(System.currentTimeMillis() + refreshTokenTtlMs)
          )
        )
      } else {
        null
      }

      // Return EndStreamAction when `includeRefreshToken` is true
      actions = actions ++ {
        if (includeRefreshToken) {
          Seq(getEndStreamAction(null, minUrlExpirationTimestamp, refreshTokenStr))
        } else {
          Nil
        }
      }
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

  // Creates a Kernel predicate for data filtering based on
  // the jsonPredicateHints specified in the request.
  // If there are any errors during the conversion do not push down predicates to Kernel.
  private def maybeCreatePredicate(
      jsonPredicateHints: Option[String],
      tableSchema: KernelStructType,
      partitionColumns: Seq[String]): Option[Predicate] = {
    val jsonPredicate = jsonPredicateHints.flatMap { json =>
      try {
        PredicateConverter.convertJsonPredicateHints(
          json,
          partitionColumns,
          JsonPredicateHintSizeLimit,
          JsonPredicateHintMaxTreeDepth,
          enableV2Predicate = true
        )(tableSchema)
      } catch {
        case e: Exception =>
          // Skip push down if the jsonPredicateHints cannot be converted.
          None
      }
    }

    val finalPredicate = jsonPredicate match {
      case Some(p1) => Some(p1)
      case _ => None
    }
    finalPredicate
  }

  private def processBatchByColumnVector(
      scanFileBatch: FilteredColumnarBatch,
      version: Long,
      queryType: QueryTypes.QueryType,
      respondedFormat: String,
      table: Table,
      engine: Engine,
      isVersionQuery: Boolean,
      snapshot: SharedTableSnapshot,
      limitHint: Option[Long],
      clientReaderFeaturesSet: Set[String]): Seq[Object] = {

    val batchData = scanFileBatch.getData
    val batchSize = batchData.getSize
    val addFileVectors = getAddFileColumnVectors(batchData)
    val selectionVector = scanFileBatch.getSelectionVector
    var addFileObjects = Seq[Object]()
    val dataPath = new Path(table.getPath(engine))

    for (rowId <- 0 until batchSize) {
      if (limitHint.exists(_ <= numRecords)) {
        earlyTermination = true
        return addFileObjects
      }
      val isSelected = !selectionVector.isPresent ||
        (!selectionVector.get.isNullAt(rowId) && selectionVector.get.getBoolean(rowId))
      if (isSelected) {
        val addFile = getResponseAddFile(
          addFileVectors,
          rowId,
          version,
          // Skip returning timestamp since we're not able to retrieve the correct
          // timestamp for the table version from Kernel. The timestamp is not
          // needed by the client in snapshot query.
          timestamp = null,
          respondedFormat,
          queryType,
          dataPath,
          clientReaderFeaturesSet
        )
        addFileObjects = addFileObjects :+ addFile
      }
    }
    addFileObjects
  }

  private def absolutePath(path: Path, child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      throw new IllegalStateException("table containing absolute paths cannot be shared")
    } else {
      new Path(path, p)
    }
  }

  // Get the column vectors for each AddFile field from the given batch.
  private def getAddFileColumnVectors(scanFileBatch: ColumnarBatch): AddFileColumnVectors = {
    // Get the ordinals of the scan file fields if it's not already done.
    // We only need to do this once because the scan file schema won't change.
    if (scanFileFieldOrdinals == null) {
      retrieveScanFileFieldOrdinals(scanFileBatch.getSchema)
    }
    val addFileVector = scanFileBatch.getColumnVector(scanFileFieldOrdinals.addFileOrdinal)
    AddFileColumnVectors(
      path = addFileVector.getChild(scanFileFieldOrdinals.addFilePathOrdinal),
      partitionValues = addFileVector.getChild(scanFileFieldOrdinals.addFilePartitionValuesOrdinal),
      size = addFileVector.getChild(scanFileFieldOrdinals.addFileSizeOrdinal),
      modificationTime = addFileVector.getChild(scanFileFieldOrdinals.addFileModTimeOrdinal),
      dataChange = addFileVector.getChild(scanFileFieldOrdinals.addFileDataChangeOrdinal),
      stats = addFileVector.getChild(scanFileFieldOrdinals.addFileStatsOrdinal),
      deletionVector = addFileVector.getChild(scanFileFieldOrdinals.addFileDvOrdinal)
    )
  }

  // Given the scan file schema, retrieve the ordinals of each scan file field and store it
  // as a global variable for later use.
  private def retrieveScanFileFieldOrdinals(scanFileSchema: KernelStructType): Unit = {
    val addFileOrdinal = getFieldOrdinal("add", scanFileSchema)
    val addFileSchema = scanFileSchema.get("add").getDataType.asInstanceOf[KernelStructType]
    scanFileFieldOrdinals = ScanFileFieldOrdinals(
      addFileOrdinal = addFileOrdinal,
      addFilePathOrdinal = getFieldOrdinal("path", addFileSchema),
      addFilePartitionValuesOrdinal = getFieldOrdinal("partitionValues", addFileSchema),
      addFileSizeOrdinal = getFieldOrdinal("size", addFileSchema),
      addFileModTimeOrdinal = getFieldOrdinal("modificationTime", addFileSchema),
      addFileDataChangeOrdinal = getFieldOrdinal("dataChange", addFileSchema),
      addFileStatsOrdinal = getFieldOrdinal("stats", addFileSchema),
      addFileDvOrdinal = getFieldOrdinal("deletionVector", addFileSchema)
    )
  }

  // Get the ordinal of the field in the specified schema.
  // Throw error if the field is not found in the schema.
  private def getFieldOrdinal(fieldName: String, schema: KernelStructType): Int = {
    val ordinal = schema.indexOf(fieldName)
    if (ordinal < 0) {
      throw new DeltaSharingUnsupportedOperationException(
        s"There's no `$fieldName` entry in the scan file schema.")
    }
    ordinal
  }

  // Construct the Delta format PartitionValues from Kernel format.
  private def getDeltaPartitionValues(
      partitionValuesVector: ColumnVector,
      rowId: Int): Map[String, String] = {
    val kernelMap = partitionValuesVector.getMap(rowId)
    val keys = kernelMap.getKeys
    val values = kernelMap.getValues
    var partitionValues = Map.empty[String, String]
    for (i <- 0 until kernelMap.getSize) {
      partitionValues += (keys.getString(i) -> values.getString(i))
    }
    partitionValues
  }


  private def decodeAndValidateRefreshToken(tokenStr: String): RefreshToken = {
    val token = try {
      DeltaSharedTableKernel.decodeToken[RefreshToken](tokenStr)
    } catch {
      case NonFatal(_) =>
        throw new DeltaSharingIllegalArgumentException(
          s"Error decoding refresh token: $tokenStr."
        )
    }
    if (token.getExpirationTimestamp < System.currentTimeMillis()) {
      throw new DeltaSharingIllegalArgumentException(
        "The refresh token has expired. Please restart the query."
      )
    }
    if (token.getId != tableConfig.id) {
      throw new DeltaSharingIllegalArgumentException(
        "The table specified in the refresh token does not match the table being queried."
      )
    }
    token
  }

  protected def getRespondedFormat(
      responseFormatSet: Set[String],
      minReaderVersion: Int): String = {
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
          schemaString = m.getSchemaString,
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

  // Construct the Delta format DeletionVectorDescriptor class from Kernel format.
  private def getDeltaDeletionVectorDescriptor(
      dvVector: ColumnVector,
      rowId: Int): DeletionVectorDescriptor = {
    val kernelDv = KernelDeletionVectorDescriptor.fromColumnVector(dvVector, rowId)
    if (kernelDv == null) {
      null
    } else {
      DeletionVectorDescriptor(
        storageType = kernelDv.getStorageType,
        pathOrInlineDv = kernelDv.getPathOrInlineDv,
        offset = Option(kernelDv.getOffset.orElse(null)),
        sizeInBytes = kernelDv.getSizeInBytes,
        cardinality = kernelDv.getCardinality
      )
    }
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

  private def getResponseAddFile(
      addFileColumnVectors: AddFileColumnVectors,
      rowId: Int,
      version: Long,
      timestamp: java.lang.Long,
      respondedFormat: String,
      queryType: QueryTypes.QueryType,
      dataPath: Path,
      clientReaderFeaturesSet: Set[String]): Object = {

    val partitionValues = getDeltaPartitionValues(addFileColumnVectors.partitionValues, rowId)
    val addFileVectorPath = addFileColumnVectors.path.getString(rowId)
    val cloudPath = absolutePath(dataPath, addFileVectorPath)
    val signedUrl = fileSigner.sign(cloudPath)
    var dvDescriptor = getDeltaDeletionVectorDescriptor(addFileColumnVectors.deletionVector, rowId)

    if (dvDescriptor != null) {
      // scalastyle:off caselocale
      if (!DeletionVectorsTableFeature.isInSet(clientReaderFeaturesSet)) {
        throw new DeltaSharingUnsupportedOperationException("Deletion Vector property disabled")
      }
      // scalastyle:on caselocale
      val dvAbsolutePath = dvDescriptor.absolutePath(dataPath)
      val presignedDvUrl = fileSigner.sign(dvAbsolutePath)
      minUrlExpirationTimestamp = minUrlExpirationTimestamp.min(presignedDvUrl.expirationTimestamp)
      dvDescriptor = dvDescriptor.copy(pathOrInlineDv = presignedDvUrl.url, storageType = "p")
    }

    minUrlExpirationTimestamp = minUrlExpirationTimestamp.min(signedUrl.expirationTimestamp)
    val size = addFileColumnVectors.size.getLong(rowId)
    val stats =
      if (addFileColumnVectors.stats.isNullAt(rowId)) null
      else addFileColumnVectors.stats.getString(rowId)

    numRecords += JsonUtils.extractNumRecords(stats).getOrElse(0L)

    if (respondedFormat == DeltaSharedTableKernel.RESPONSE_FORMAT_DELTA) {
      DeltaResponseFileAction(
        // Using sha256 instead of m5 because it's less likely to have key collision.
        id = Hashing.sha256().hashString(addFileVectorPath, UTF_8).toString,
        // `version` is left empty in latest snapshot query
        version = if (queryType == QueryTypes.QueryLatestSnapshot) null else version,
        timestamp = timestamp,
        expirationTimestamp = signedUrl.expirationTimestamp,
        deltaSingleAction = DeltaSingleAction(
          add = DeltaAddFile(
            path = signedUrl.url,
            partitionValues = partitionValues,
            size = size,
            modificationTime = addFileColumnVectors.modificationTime.getLong(rowId),
            dataChange = addFileColumnVectors.dataChange.getBoolean(rowId),
            stats = stats,
            deletionVector = dvDescriptor
          )
        )
      ).wrap
    } else {
      AddFile(
        url = signedUrl.url,
        id = Hashing.md5().hashString(addFileVectorPath, UTF_8).toString,
        expirationTimestamp = signedUrl.expirationTimestamp,
        partitionValues = partitionValues,
        size = size,
        stats = stats,
        version = if (queryType == QueryTypes.QueryLatestSnapshot) null else version,
        timestamp = timestamp
      ).wrap
    }
  }

  // Construct and return the end action of the streaming response.
  private def getEndStreamAction(
      nextPageTokenStr: String,
      minUrlExpirationTimestamp: Long,
      refreshTokenStr: String = null): SingleAction = {
    EndStreamAction(
      refreshTokenStr,
      nextPageTokenStr,
      if (minUrlExpirationTimestamp == Long.MaxValue) null else minUrlExpirationTimestamp
    ).wrap
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
    kernelSnapshot: SnapshotImpl)

private case class AddFileColumnVectors(
    path: ColumnVector,
    partitionValues: ColumnVector,
    size: ColumnVector,
    modificationTime: ColumnVector,
    dataChange: ColumnVector,
    stats: ColumnVector,
    deletionVector: ColumnVector)

private case class ScanFileFieldOrdinals(
    addFileOrdinal: Int,
    addFilePathOrdinal: Int,
    addFilePartitionValuesOrdinal: Int,
    addFileSizeOrdinal: Int,
    addFileModTimeOrdinal: Int,
    addFileDataChangeOrdinal: Int,
    addFileStatsOrdinal: Int,
    addFileDvOrdinal: Int)
