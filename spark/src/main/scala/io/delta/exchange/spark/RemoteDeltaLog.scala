package io.delta.exchange.spark

import scala.collection.JavaConverters._
import java.io.{File, FileNotFoundException}
import java.net.{URI, URL}
import java.util

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.google.common.io.Files
import io.delta.exchange.client.{DeltaExchangeClient, DeltaExchangeProfile, DeltaExchangeProfileProvider, DeltaExchangeRestClient, JsonUtils}
import org.apache.hadoop.fs.{FileStatus, Path}
import io.delta.exchange.client.model.{AddFile, Metadata, Protocol, Table => SharedTable}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Cast, Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{FileFormat, FileIndex, HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Dataset, Encoder, SQLContext, SparkSession}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet, Cast, Expression, ExpressionSet, GenericInternalRow, Literal, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{FileFormat, FileIndex, HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

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
class RemoteDeltaLog(table: SharedTable, path: Path, client: DeltaExchangeClient) {

  @volatile private var currentSnapshot: RemoteSnapshot = new RemoteSnapshot(client, table)

  protected def spark = SparkSession.active

  def snapshot: RemoteSnapshot = currentSnapshot

  def update(): Unit = synchronized {
    currentSnapshot = new RemoteSnapshot(client, table)
  }

  def createRelation(): BaseRelation = {
    val snapshotToUse = snapshot
    val fileIndex = new RemoteTahoeLogFileIndex(spark, this, path, snapshotToUse)
    new HadoopFsRelation(
      fileIndex,
      partitionSchema = snapshotToUse.partitionSchema,
      dataSchema = snapshotToUse.schema,
      bucketSpec = None,
      snapshotToUse.fileFormat,
      Map.empty)(spark)
  }
}

class DeltaExchangeFileProfileProvider(file: String) extends DeltaExchangeProfileProvider {
  val profile = {
    val conf = SparkSession.getActiveSession.map(_.sessionState.newHadoopConf()).getOrElse(new Configuration)
    val input = new Path(file).getFileSystem(conf).open(new Path(file))
    try {
      JsonUtils.fromJson[DeltaExchangeProfile](IOUtils.toString(input, "UTF-8"))
    } finally {
      input.close()
    }
  }

  override def getProfile: DeltaExchangeProfile = profile
}

object RemoteDeltaLog {
  private lazy val _addFileEncoder: ExpressionEncoder[AddFile] = ExpressionEncoder[AddFile]()

  implicit def addFileEncoder: Encoder[AddFile] = {
    _addFileEncoder.copy()
  }

  def apply(path: String, tokenFile: Option[String]): RemoteDeltaLog = {
    val sslTrustAll =
      SparkSession.active.sessionState.conf
        .getConfString("spark.delta.exchange.client.sslTrustAll", "false").toBoolean
    val shapeIndex = path.lastIndexOf('#')
    if (shapeIndex < 0) {
      throw new IllegalArgumentException(s"path $path is not valid")
    }
    val profileFile = path.substring(0, shapeIndex)
    val profileProvider = new DeltaExchangeFileProfileProvider(profileFile)
    val tableSplits = path.substring(shapeIndex + 1).split("\\.")
    if (tableSplits.length != 3) {
      throw new IllegalArgumentException(s"path $path is not valid")
    }

    val table = SharedTable(tableSplits(2), tableSplits(1), tableSplits(0))
    val client = new DeltaExchangeRestClient(profileProvider, sslTrustAll)
    new RemoteDeltaLog(table, new Path(path), client)
//    val table = RemoteDeltaTable(path, tokenFile)
//    val localClientPath = SparkSession.active.conf.getOption("delta.exchange.localClient.path")
//    val client = localClientPath.map { path =>
//      // This is a local client for testing purposes. Only these two tokens are valid
//      assert(Seq("token", "tokenFromLocalFile").contains(table.apiToken))
//      val tablePath = new Path(path, table.uuid)
//      if (path.startsWith("s3")) {
//        val constructor = Class.forName("io.delta.exchange.spark.DeltaLogLocalClient")
//          .getConstructor(classOf[Path], classOf[CloudFileSigner])
//        constructor.newInstance(tablePath, TestResource.AWS.signer).asInstanceOf[DeltaExchangeClient]
//      } else if (path.startsWith("gs")) {
//        val constructor = Class.forName("io.delta.exchange.spark.DeltaLogLocalClient")
//          .getConstructor(classOf[Path], classOf[CloudFileSigner])
//        constructor.newInstance(tablePath, TestResource.GCP.signer).asInstanceOf[DeltaExchangeClient]
//      } else if (path.startsWith("wasb")) {
//        val constructor = Class.forName("io.delta.exchange.spark.DeltaLogLocalClient")
//          .getConstructor(classOf[Path], classOf[CloudFileSigner])
//        constructor.newInstance(tablePath, TestResource.Azure.signer).asInstanceOf[DeltaExchangeClient]
//      } else {
//        val constructor = Class.forName("io.delta.exchange.spark.DeltaLogLocalClient")
//          .getConstructor(classOf[Path], classOf[CloudFileSigner])
//        constructor.newInstance(tablePath, new DummyFileSigner).asInstanceOf[DeltaExchangeClient]
//      }
//    } getOrElse {
//      new SparkDeltaExchangeRestClient
//    }
//    val tableInfo = client.getTableInfo(GetTableInfoRequest(table.uuid))
//    new RemoteDeltaLog(table.uuid, tableInfo.version, new Path(tableInfo.path), client)
  }
}

class RemoteSnapshot(client: DeltaExchangeClient, table: SharedTable) {

  protected def spark = SparkSession.active

  lazy val (metadata, protocol) = {
    val tableMetadata = client.getMetadata(table)
    (tableMetadata.metadata, tableMetadata.protocol)
  }

  lazy val schema: StructType =
    Option(metadata.schemaString).map { s =>
      DataType.fromJson(s).asInstanceOf[StructType]
    }.getOrElse(StructType.apply(Nil))

  lazy val partitionSchema = new StructType(metadata.partitionColumns.map(c => schema(c)).toArray)

  def fileFormat: FileFormat = new ParquetFileFormat()

  val sizeInBytes: Long = 0

  lazy val allFiles: Dataset[AddFile] = {
    val implicits = spark.implicits
    import implicits._
    val tableFiles = client.getFiles(table, Nil, None)
    checkProtocolNotChange(tableFiles.protocol)
    checkSchemaNotChange(tableFiles.metadata)
    tableFiles.files.toDS()
  }

  private def checkProtocolNotChange(newProtocol: Protocol): Unit = {
    if (newProtocol != protocol)
      throw new RuntimeException(
        s"""The table protocol has changed since your DataFrame was created. Please redefine your DataFrame""")
  }

  private def checkSchemaNotChange(newMetadata: Metadata): Unit = {
    if (newMetadata.schemaString != metadata.schemaString || newMetadata.partitionColumns != metadata.partitionColumns)
      throw new RuntimeException(
        s"""The schema or partition columns of your Delta table has changed since your
           |DataFrame was created. Please redefine your DataFrame""")
  }

  def filesForScan(projection: Seq[Attribute], filters: Seq[Expression]): Seq[AddFile] = {
    implicit val enc = RemoteDeltaLog.addFileEncoder

    val partitionFilters = filters.flatMap { filter =>
      DeltaTableUtils.splitMetadataAndDataPredicates(filter, metadata.partitionColumns, spark)._1
    }

    val predicates = partitionFilters.map(_.sql)

    val remoteFiles = {
      val implicits = spark.implicits
      import implicits._
      val tableFiles = client.getFiles(table, predicates, None)

      checkProtocolNotChange(tableFiles.protocol)
      checkSchemaNotChange(tableFiles.metadata)
      tableFiles.files.toDS()
    }

    filterFileList(
      partitionSchema,
      remoteFiles.toDF,
      partitionFilters).as[AddFile].collect()
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

  override def shortName() = "delta_sharing"
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
      .map(f => toDeltaPath(f).toString)
      .toArray
  }

  override def refresh(): Unit = {}
  override val sizeInBytes: Long = deltaLog.snapshot.sizeInBytes

  override def partitionSchema: StructType = snapshotAtAnalysis.partitionSchema

  override def rootPaths: Seq[Path] = path :: Nil

  private def toDeltaPath(f: AddFile): Path = {
    DeltaFileSystem.createPath(new URI(f.url), f.size)
  }

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val timeZone = spark.sessionState.conf.sessionLocalTimeZone
    snapshotAtAnalysis.filesForScan(projection = Nil, partitionFilters ++ dataFilters)
      .groupBy(_.partitionValues).map {
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
              /* modificationTime */ 0,
              toDeltaPath(f))
          }.toArray

          PartitionDirectory(new GenericInternalRow(rowValues), fileStats)
      }.toSeq
  }
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
