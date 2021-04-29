package io.delta.exchange.spark

import scala.collection.JavaConverters._
import java.io.FileNotFoundException
import java.net.{URI, URL}
import java.util

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.delta.exchange.spark.actions._
import io.delta.exchange.client.{DeltaExchangeClient, GetFilesRequest, GetMetadataRequest, GetTableInfoRequest, JsonUtils}
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Cast, Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{FileFormat, FileIndex, HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession, SQLContext}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet, Cast, Expression, ExpressionSet, GenericInternalRow, Literal, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{FileFormat, FileIndex, HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SQLContext, SparkSession}

import scala.util.{Failure, Success, Try}

case class RemoteDeltaTable(apiUrl: URL, apiToken: String, uuid: String)

object RemoteDeltaTable {
  def apply(path: String, tokenFile: Option[String] = None): RemoteDeltaTable = {
    // delta://uuid:token@apiurl
    val uri = new URI(path)
    assert(uri.getScheme == "delta")
    val Array(uuid, apiToken) = tokenFile match {
      case Some(tokenFilePath) =>
        val file = scala.io.Source.fromFile(tokenFilePath)
        try {
          val itr = file.getLines()
          assert(itr.hasNext)
          Array(uri.getUserInfo, itr.next())
        } finally {
          file.close()
        }
      case None => uri.getUserInfo.split(":")
    }
    val host = uri.getHost
    RemoteDeltaTable(new URL(s"https://$host"), apiToken, uuid)
  }
}

// scalastyle:off
class RemoteDeltaLog(uuid: String, initialVersion: Long, path: Path, client: DeltaExchangeClient) {

  @volatile private var currentSnapshot: RemoteSnapshot =
    new RemoteSnapshot(client, uuid, initialVersion)

  protected def spark = SparkSession.active

  def snapshot: RemoteSnapshot = currentSnapshot

  def update(): Unit = synchronized {
    val tableInfo = client.getTableInfo(GetTableInfoRequest(uuid))
    currentSnapshot = new RemoteSnapshot(client, uuid, tableInfo.version)
  }

  def createRelation(): BaseRelation = {
    val snapshotToUse = snapshot
    if (snapshotToUse.version < 0) {
      throw new FileNotFoundException(path.toString)
    }
    val fileIndex = new RemoteTahoeLogFileIndex(spark, this, path, snapshotToUse)
    new HadoopFsRelation(
      fileIndex,
      partitionSchema = snapshotToUse.metadata.partitionSchema,
      dataSchema = snapshotToUse.metadata.schema,
      bucketSpec = None,
      snapshotToUse.fileFormat,
      snapshotToUse.metadata.format.options)(spark)
  }
}

object RemoteDeltaLog {
  def apply(path: String, tokenFile: Option[String]): RemoteDeltaLog = {
    val table = RemoteDeltaTable(path, tokenFile)
    val localClientPath = SparkSession.active.conf.getOption("delta.exchange.localClient.path")
    val client = localClientPath.map { path =>
      // This is a local client for testing purposes. Only these two tokens are valid
      assert(Seq("token", "tokenFromLocalFile").contains(table.apiToken))
      val tablePath = new Path(path, table.uuid)
      if (path.startsWith("s3")) {
        val constructor = Class.forName("io.delta.exchange.spark.DeltaLogLocalClient")
          .getConstructor(classOf[Path], classOf[CloudFileSigner])
        constructor.newInstance(tablePath, TestResource.AWS.signer).asInstanceOf[DeltaExchangeClient]
      } else if (path.startsWith("gs")) {
        val constructor = Class.forName("io.delta.exchange.spark.DeltaLogLocalClient")
          .getConstructor(classOf[Path], classOf[CloudFileSigner])
        constructor.newInstance(tablePath, TestResource.GCP.signer).asInstanceOf[DeltaExchangeClient]
      } else if (path.startsWith("wasb")) {
        val constructor = Class.forName("io.delta.exchange.spark.DeltaLogLocalClient")
          .getConstructor(classOf[Path], classOf[CloudFileSigner])
        constructor.newInstance(tablePath, TestResource.Azure.signer).asInstanceOf[DeltaExchangeClient]
      } else {
        val constructor = Class.forName("io.delta.exchange.spark.DeltaLogLocalClient")
          .getConstructor(classOf[Path], classOf[CloudFileSigner])
        constructor.newInstance(tablePath, new DummyFileSigner).asInstanceOf[DeltaExchangeClient]
      }
    } getOrElse {
      new SparkDeltaExchangeRestClient
    }
    val tableInfo = client.getTableInfo(GetTableInfoRequest(table.uuid))
    new RemoteDeltaLog(table.uuid, tableInfo.version, new Path(tableInfo.path), client)
  }
}

class RemoteSnapshot(client: DeltaExchangeClient, uuid: String, val version: Long) {

  protected def spark = SparkSession.active

  lazy val metadata: Metadata = {
    JsonUtils.fromJson[Metadata](client.getMetadata(GetMetadataRequest(uuid, version)).metadata)
  }

  def fileFormat: FileFormat = new ParquetFileFormat()

  val sizeInBytes: Long = 0

  lazy val allFiles: Dataset[AddFile] = {
    val implicits = spark.implicits
    import implicits._
    client.getFiles(GetFilesRequest(uuid, version, None)).file.map { addFileJson =>
      val addFile = JsonUtils.fromJson[AddFile](addFileJson)
      val deltaPath = DeltaFileSystem.createPath(new URI(addFile.path), addFile.size)
      addFile.copy(path = deltaPath.toUri.toString)
    }.toDS()
  }

  def filesForScan(projection: Seq[Attribute], filters: Seq[Expression]): DeltaScan = {
    implicit val enc = SingleAction.addFileEncoder

    val partitionFilters = filters.flatMap { filter =>
      DeltaTableUtils.splitMetadataAndDataPredicates(filter, metadata.partitionColumns, spark)._1
    }

    val partitionFilterSql = partitionFilters.reduceLeftOption(And).map(_.sql)

    println("partitionFilterSql: " + partitionFilterSql)

    val remoteFiles = {
      val implicits = spark.implicits
      import implicits._
      client.getFiles(GetFilesRequest(uuid, version, partitionFilterSql)).file.map { addFileJson =>
        val addFile = JsonUtils.fromJson[AddFile](addFileJson)
        val deltaPath = DeltaFileSystem.createPath(new URI(addFile.path), addFile.size)
        addFile.copy(path = deltaPath.toUri.toString)
      }.toDS()
    }.toDF

    val files = filterFileList(
      metadata.partitionSchema,
      remoteFiles,
      partitionFilters).as[AddFile].collect()

    DeltaScan(version = version, files, null, null, null)(null, null, null, null)
  }

  def filterFileList(
    partitionSchema: StructType,
    files: DataFrame,
    partitionFilters: Seq[Expression],
    partitionColumnPrefixes: Seq[String] = Nil): DataFrame = {
    val rewrittenFilters = rewritePartitionFilters(
      partitionSchema,
      files.sparkSession.sessionState.conf.resolver,
      partitionFilters,
      partitionColumnPrefixes)
    val columnFilter = new Column(rewrittenFilters.reduceLeftOption(And).getOrElse(Literal(true)))
    files.filter(columnFilter)
  }

  /**
   * Rewrite the given `partitionFilters` to be used for filtering partition values.
   * We need to explicitly resolve the partitioning columns here because the partition columns
   * are stored as keys of a Map type instead of attributes in the AddFile schema (below) and thus
   * cannot be resolved automatically.
   *
   * @param partitionFilters Filters on the partition columns
   * @param partitionColumnPrefixes The path to the `partitionValues` column, if it's nested
   */
  def rewritePartitionFilters(
    partitionSchema: StructType,
    resolver: Resolver,
    partitionFilters: Seq[Expression],
    partitionColumnPrefixes: Seq[String] = Nil): Seq[Expression] = {
    partitionFilters.map(_.transformUp {
      case a: Attribute =>
        // If we have a special column name, e.g. `a.a`, then an UnresolvedAttribute returns
        // the column name as '`a.a`' instead of 'a.a', therefore we need to strip the backticks.
        val unquoted = a.name.stripPrefix("`").stripSuffix("`")
        val partitionCol = partitionSchema.find { field => resolver(field.name, unquoted) }
        partitionCol match {
          case Some(StructField(name, dataType, _, _)) =>
            Cast(
              UnresolvedAttribute(partitionColumnPrefixes ++ Seq("partitionValues", name)),
              dataType)
          case None =>
            // This should not be able to happen, but the case was present in the original code so
            // we kept it to be safe.
            UnresolvedAttribute(partitionColumnPrefixes ++ Seq("partitionValues", a.name))
        }
    })
  }
}

class DeltaExchangeDataSource extends TableProvider with RelationProvider with DataSourceRegister {

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
      properties: util.Map[String, String]): Table = {
    // Return a Table with no capabilities so we fall back to the v1 path.
    new Table {
      override def name(): String = s"V1FallbackTable"

      override def schema(): StructType = new StructType()

      override def capabilities(): util.Set[TableCapability] = Set.empty[TableCapability].asJava
    }
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse(
      "path", throw new IllegalArgumentException("'path' is not specified"))
    val tokenFile = parameters.get("tokenFile")
    val deltaLog = RemoteDeltaLog(path, tokenFile)
    deltaLog.createRelation()
  }

  override def shortName() = "delta-exchange"
}

class DeltaExchangeTable(path: String) extends Table {
  override def name(): String = s"delta://$path"

  override def schema(): StructType = new StructType()

  override def capabilities(): util.Set[TableCapability] = Set.empty[TableCapability].asJava
}

class RemoteTahoeLogFileIndex(
    spark: SparkSession,
    deltaLog: RemoteDeltaLog,
    path: Path,
    snapshotAtAnalysis: RemoteSnapshot) extends FileIndex {

  protected def absolutePath(child: String): Path = {
    val p = new Path(new URI(child))
    assert(p.isAbsolute)
    p
  }

  override def inputFiles: Array[String] = {
    snapshotAtAnalysis.filesForScan(projection = Nil, Nil)
      .files
      .map(f => absolutePath(f.path).toString)
      .toArray
  }

  override def refresh(): Unit = {}
  override val sizeInBytes: Long = deltaLog.snapshot.sizeInBytes

  override def partitionSchema: StructType = snapshotAtAnalysis.metadata.partitionSchema

  override def rootPaths: Seq[Path] = path :: Nil

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val timeZone = spark.sessionState.conf.sessionLocalTimeZone
    snapshotAtAnalysis.filesForScan(projection = Nil, partitionFilters ++ dataFilters)
      .files.groupBy(_.partitionValues).map {
        case (partitionValues, files) =>
          val rowValues: Array[Any] = partitionSchema.map { p =>
            Cast(Literal(partitionValues(p.name)), p.dataType, Option(timeZone)).eval()
          }.toArray

          val fileStats = files.map { f =>
            new FileStatus(
              /* length */ f.size,
              /* isDir */ false,
              /* blockReplication */ 0,
              /* blockSize */ 1,
              /* modificationTime */ f.modificationTime,
              absolutePath(f.path))
          }.toArray

          PartitionDirectory(new GenericInternalRow(rowValues), fileStats)
      }.toSeq
  }
}

/**
 * Used to hold details the files and stats for a scan where we have already
 * applied filters and a limit.
 */
case class DeltaScan(
  version: Long,
  files: Seq[AddFile],
  total: DataSize,
  partition: DataSize,
  scanned: DataSize)(
  // Moved to separate argument list, to not be part of case class equals check -
  // expressions can differ by exprId or ordering, but as long as same files are scanned, the
  // PreparedDeltaFileIndex and HadoopFsRelation should be considered equal for reuse purposes.
  val partitionFilters: ExpressionSet,
  val dataFilters: ExpressionSet,
  val unusedFilters: ExpressionSet,
  val projection: AttributeSet) {
  def allFilters: ExpressionSet = partitionFilters ++ dataFilters ++ unusedFilters
}

object DeltaTableUtils extends PredicateHelper {

  /** Find the root of a Delta table from the provided path. */
  def findDeltaTableRoot(
    spark: SparkSession,
    path: Path,
    options: Map[String, String] = Map.empty): Option[Path] = {
    val fs = path.getFileSystem(spark.sessionState.newHadoopConfWithOptions(options))
    var currentPath = path
    while (currentPath != null && currentPath.getName != "_delta_log" &&
      currentPath.getName != "_samples") {
      val deltaLogPath = new Path(currentPath, "_delta_log")
      if (Try(fs.exists(deltaLogPath)).getOrElse(false)) {
        return Option(currentPath)
      }
      currentPath = currentPath.getParent
    }
    None
  }

  /** Whether a path should be hidden for delta-related file operations, such as Vacuum and Fsck. */
  def isHiddenDirectory(partitionColumnNames: Seq[String], pathName: String): Boolean = {
    // Names of the form partitionCol=[value] are partition directories, and should be
    // GCed even if they'd normally be hidden. The _db_index directory contains (bloom filter)
    // indexes and these must be GCed when the data they are tied to is GCed.
    (pathName.startsWith(".") || pathName.startsWith("_")) &&
      !pathName.startsWith("_delta_index") && !pathName.startsWith("__cdc_type") &&
      !partitionColumnNames.exists(c => pathName.startsWith(c ++ "="))
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
}

case class DataSize(
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  bytesCompressed: Option[Long] = None,
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  rows: Option[Long] = None)
