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
import java.text.SimpleDateFormat
import java.util.{TimeZone, UUID}

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.delta.sharing.{CachedTableManager, TableRefreshResult}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, Encoder, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Cast, Expression, Literal, SubqueryExpression}
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import io.delta.sharing.client.{DeltaSharingClient, DeltaSharingRestClient}
import io.delta.sharing.client.model.{AddFile, CDFColumnInfo, DeltaTableMetadata, Metadata, Protocol, Table => DeltaSharingTable}
import io.delta.sharing.client.util.ConfUtils
import io.delta.sharing.spark.perf.DeltaSharingLimitPushDown


/**
 * Used to query the current state of the transaction logs of a remote shared Delta table.
 *
 * @param initDeltaTableMetadata Used to initialize RemoteSnapshot, and is always preferred to use
 *                               if set, to avoid an unnecessary call with additional delay.
 *                               It's because we'd like to fetch the table metadata for a pre-check
 *                               to decide whether to use parquet format sharing or delta format
 *                               sharing before initializing RemoteDeltaLog.
 */
private[sharing] class RemoteDeltaLog(
  val table: DeltaSharingTable,
  val path: Path,
  val client: DeltaSharingClient,
  initDeltaTableMetadata: Option[DeltaTableMetadata] = None) {

  @volatile private var currentSnapshot: RemoteSnapshot = new RemoteSnapshot(
    path, client, table, initDeltaTableMetadata = initDeltaTableMetadata)

  def snapshot(
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None): RemoteSnapshot = {
    if (versionAsOf.isEmpty && timestampAsOf.isEmpty) {
      currentSnapshot
    } else {
      new RemoteSnapshot(path, client, table, versionAsOf, timestampAsOf, initDeltaTableMetadata)
    }
  }

  /** Update the current snapshot to the latest version. */
  def update(): Unit = synchronized {
    if (client.getTableVersion(table) != currentSnapshot.version) {
      currentSnapshot = new RemoteSnapshot(path, client, table)
    }
  }

  def createRelation(
      versionAsOf: Option[Long],
      timestampAsOf: Option[String],
      cdfOptions: Map[String, String]): BaseRelation = {
    val spark = SparkSession.active
    val snapshotToUse = snapshot(versionAsOf, timestampAsOf)
    if (!cdfOptions.isEmpty) {
      return RemoteDeltaCDFRelation(
        spark,
        snapshotToUse,
        client,
        table,
        cdfOptions
      )
    }

    val params = new RemoteDeltaFileIndexParams(spark, snapshotToUse, client.getProfileProvider)
    val fileIndex = new RemoteDeltaSnapshotFileIndex(params, None)
    if (ConfUtils.limitPushdownEnabled(spark.sessionState.conf)) {
      DeltaSharingLimitPushDown.setup(spark)
    }
    new HadoopFsRelation(
      fileIndex,
      partitionSchema = snapshotToUse.partitionSchema,
      dataSchema = snapshotToUse.schema,
      bucketSpec = None,
      snapshotToUse.fileFormat,
      Map.empty)(spark)
  }
}

private[sharing] object RemoteDeltaLog {
  private lazy val _addFileEncoder: ExpressionEncoder[AddFile] = ExpressionEncoder[AddFile]()

  implicit def addFileEncoder: Encoder[AddFile] = {
    _addFileEncoder.copy()
  }

  // Get a unique string composed of a formatted timestamp and an uuid.
  // Used as a suffix for the table name and its delta log path of a delta sharing table in a
  // streaming job, to avoid overwriting the delta log from multiple references of the same delta
  // sharing table in one streaming job.
  private[sharing] def getFormattedTimestampWithUUID(): String = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss")
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    val formattedDateTime = dateFormat.format(System.currentTimeMillis())
    val uuid = UUID.randomUUID().toString().split('-').head
    s"_${formattedDateTime}_${uuid}"
  }

  def apply(
      path: String,
      forStreaming: Boolean = false,
      responseFormat: String = DeltaSharingOptions.RESPONSE_FORMAT_PARQUET,
      initDeltaTableMetadata: Option[DeltaTableMetadata] = None): RemoteDeltaLog = {
    val parsedPath = DeltaSharingRestClient.parsePath(path)
    val client = DeltaSharingRestClient(parsedPath.profileFile, forStreaming, responseFormat)
    val deltaSharingTable = DeltaSharingTable(
      name = parsedPath.table,
      schema = parsedPath.schema,
      share = parsedPath.share
    )
    new RemoteDeltaLog(
      deltaSharingTable,
      new Path(path + getFormattedTimestampWithUUID),
      client,
      initDeltaTableMetadata
    )
  }
}

/** An immutable snapshot of a Delta table at some delta version */
class RemoteSnapshot(
    tablePath: Path,
    client: DeltaSharingClient,
    table: DeltaSharingTable,
    versionAsOf: Option[Long] = None,
    timestampAsOf: Option[String] = None,
    initDeltaTableMetadata: Option[DeltaTableMetadata] = None) extends Logging {

  protected def spark = SparkSession.active

  // initDeltaTableMetadata is from the initialization of RemoteDeltaLog and RemoteSnapshot.
  // It's always preferred to use if set, to avoid an unnecessary call with additional delay.
  // The reason is we'd like to fetch the table metadata for a pre-check to decide whether to
  // use parquet format sharing or delta format sharing.
  private lazy val tableMetadata = initDeltaTableMetadata.getOrElse(
    client.getMetadata(table, versionAsOf, timestampAsOf)
  )

  lazy val (metadata, protocol, version) = {
    (tableMetadata.metadata, tableMetadata.protocol, tableMetadata.version)
  }

  lazy val schema: StructType = DeltaTableUtils.toSchema(metadata.schemaString)

  lazy val partitionSchema = new StructType(metadata.partitionColumns.map(c => schema(c)).toArray)

  def fileFormat: FileFormat = new ParquetFileFormat()

  def getTablePath: Path = tablePath

  // This function is invoked during spark's query planning phase.
  //
  // The delta sharing server may not return the table size in its metadata if:
  //   - We are talking to an older version of the server.
  //   - The table does not contain this information in its metadata.
  // We perform a full scan in that case.
  lazy val sizeInBytes: Long = {
    val implicits = spark.implicits
    import implicits._

    if (metadata.size != null) {
      metadata.size
    } else {
      log.warn("Getting table size from a full file scan for table: " + table)
      val tableFiles = client.getFiles(
        table = table,
        predicates = Nil,
        limit = None,
        versionAsOf,
        timestampAsOf,
        jsonPredicateHints = None,
        refreshToken = None
      )
      checkProtocolNotChange(tableFiles.protocol)
      checkSchemaNotChange(tableFiles.metadata)
      tableFiles.files.map(_.size).sum
    }
  }

  private def checkProtocolNotChange(newProtocol: Protocol): Unit = {
    if (newProtocol != protocol) {
      throw new SparkException(
        "The table protocol has changed since your DataFrame was created. " +
          "Please redefine your DataFrame")
    }
  }

  private def checkSchemaNotChange(newMetadata: Metadata): Unit = {
    if (newMetadata.schemaString != metadata.schemaString ||
      newMetadata.partitionColumns != metadata.partitionColumns) {
      throw new SparkException(
        s"""The schema or partition columns of your Delta table has changed since your
           |DataFrame was created. Please redefine your DataFrame""")
    }
  }

  def filesForScan(
      filters: Seq[Expression],
      limitHint: Option[Long],
      jsonPredicateHints: Option[String],
      fileIndex: RemoteDeltaSnapshotFileIndex): Seq[AddFile] = {
    implicit val enc = RemoteDeltaLog.addFileEncoder

    val partitionFilters = filters.flatMap { filter =>
      DeltaTableUtils.splitMetadataAndDataPredicates(filter, metadata.partitionColumns, spark)._1
    }

    val rewrittenFilters = DeltaTableUtils.rewritePartitionFilters(
      partitionSchema,
      spark.sessionState.conf.resolver,
      partitionFilters,
      spark.sessionState.conf.sessionLocalTimeZone)

    val predicates = rewrittenFilters.map(_.sql)
    if (predicates.nonEmpty) {
      logDebug(s"Sending predicates $predicates to the server")
    }

    val remoteFiles = {
      val implicits = spark.implicits
      import implicits._
      val tableFiles = client.getFiles(
        table, predicates, limitHint, versionAsOf, timestampAsOf, jsonPredicateHints, None
      )
      var minUrlExpirationTimestamp: Option[Long] = None
      val idToUrl = tableFiles.files.map { file =>
        if (file.expirationTimestamp != null) {
          minUrlExpirationTimestamp = if (minUrlExpirationTimestamp.isDefined &&
            minUrlExpirationTimestamp.get < file.expirationTimestamp) {
            minUrlExpirationTimestamp
          } else {
            Some(file.expirationTimestamp)
          }
        }
        file.id -> file.url
      }.toMap
      CachedTableManager.INSTANCE
        .register(
          fileIndex.params.path.toString,
          idToUrl,
          Seq(new WeakReference(fileIndex)),
          fileIndex.params.profileProvider,
          refreshToken => {
            val tableFiles = client.getFiles(
              table, Nil, None, versionAsOf, timestampAsOf, jsonPredicateHints, refreshToken
            )
            var minUrlExpiration: Option[Long] = None
            val idToUrl = tableFiles.files.map { add =>
              if (add.expirationTimestamp != null) {
                minUrlExpiration = if (minUrlExpiration.isDefined
                  && minUrlExpiration.get < add.expirationTimestamp) {
                  minUrlExpiration
                } else {
                  Some(add.expirationTimestamp)
                }
              }
              add.id -> add.url
            }.toMap
            TableRefreshResult(idToUrl, minUrlExpiration, tableFiles.refreshToken)
          },
          if (CachedTableManager.INSTANCE.isValidUrlExpirationTime(minUrlExpirationTimestamp)) {
            minUrlExpirationTimestamp.get
          } else {
            System.currentTimeMillis() + CachedTableManager.INSTANCE.preSignedUrlExpirationMs
          },
          tableFiles.refreshToken
        )
      checkProtocolNotChange(tableFiles.protocol)
      checkSchemaNotChange(tableFiles.metadata)
      tableFiles.files.toDS()
    }

    val columnFilter = new Column(rewrittenFilters.reduceLeftOption(And).getOrElse(Literal(true)))
    remoteFiles.filter(columnFilter).as[AddFile].collect()
  }
}

// scalastyle:off
/** Fork from Delta Lake: https://github.com/delta-io/delta/blob/v0.8.0/src/main/scala/org/apache/spark/sql/delta/DeltaTable.scala#L76 */
// scalastyle:on
private[sharing] object DeltaTableUtils {

  def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  /**
   * Does the predicate only contains partition columns?
   */
  def isPredicatePartitionColumnsOnly(
      condition: Expression,
      partitionColumns: Seq[String],
      spark: SparkSession): Boolean = {
    val nameEquality = spark.sessionState.analyzer.resolver
    condition.references.forall { r =>
      partitionColumns.exists(nameEquality(r.name, _))
    }
  }

  /**
   * Partition the given condition into two sequence of conjunctive predicates:
   * - predicates that can be evaluated using metadata only.
   * - other predicates.
   */
  def splitMetadataAndDataPredicates(
      condition: Expression,
      partitionColumns: Seq[String],
      spark: SparkSession): (Seq[Expression], Seq[Expression]) = {
    splitConjunctivePredicates(condition).partition(
      isPredicateMetadataOnly(_, partitionColumns, spark))
  }

  /**
   * Check if condition involves a subquery expression.
   */
  def containsSubquery(condition: Expression): Boolean = {
    SubqueryExpression.hasSubquery(condition)
  }

  /**
   * Check if condition can be evaluated using only metadata. In Delta, this means the condition
   * only references partition columns and involves no subquery.
   */
  def isPredicateMetadataOnly(
      condition: Expression,
      partitionColumns: Seq[String],
      spark: SparkSession): Boolean = {
    isPredicatePartitionColumnsOnly(condition, partitionColumns, spark) &&
      !containsSubquery(condition)
  }

  // Converts schema string to schema struct.
  def toSchema(schemaString: String): StructType = {
    Option(schemaString).map { s =>
      DataType.fromJson(s).asInstanceOf[StructType]
    }.getOrElse(StructType.apply(Nil))
  }

  // Updates input schema with the new fields.
  def updateSchema(origSchema: StructType, newFields: Map[String, DataType]): StructType = {
    var schema: StructType = origSchema
    newFields.forall(s => {
      schema = schema.add(s._1, s._2)
      true
    })
    schema
  }

  // Adds cdc schema to the table schema.
  def addCdcSchema(tableSchema: StructType): StructType = {
    updateSchema(tableSchema, CDFColumnInfo.getInternalPartitonSchemaForCDFAddRemoveFile())
  }

  // Adds cdc schema to the table schema string.
  def addCdcSchema(tableSchemaStr: String): StructType = {
    updateSchema(
      toSchema(tableSchemaStr),
      CDFColumnInfo.getInternalPartitonSchemaForCDFAddRemoveFile()
    )
  }

  /**
   * Rewrite the given `partitionFilters` to be used for filtering partition values.
   * We need to explicitly resolve the partitioning columns here because the partition columns
   * are stored as keys of a Map type instead of attributes in the AddFile schema (below) and thus
   * cannot be resolved automatically.
   */
  def rewritePartitionFilters(
    partitionSchema: StructType,
    resolver: Resolver,
    partitionFilters: Seq[Expression],
    timeZone: String): Seq[Expression] = {
    partitionFilters.map(_.transformUp {
      case a: Attribute =>
        // If we have a special column name, e.g. `a.a`, then an UnresolvedAttribute returns
        // the column name as '`a.a`' instead of 'a.a', therefore we need to strip the backticks.
        val unquoted = a.name.stripPrefix("`").stripSuffix("`")
        val partitionCol = partitionSchema.find { field => resolver(field.name, unquoted) }
        partitionCol match {
          case Some(StructField(name, dataType, _, _)) =>
            new Cast(UnresolvedAttribute(Seq("partitionValues", name)), dataType, Option(timeZone))
          case None =>
            // This should not be able to happen, but the case was present in the original code so
            // we kept it to be safe.
            UnresolvedAttribute(Seq("partitionValues", a.name))
        }
    })
  }
}
