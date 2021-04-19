package io.delta.exchange.spark

import java.net.{URI, URL}
import java.util

import org.apache.spark.sql.delta._
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Cast, Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.delta.actions.{AddFile, Metadata, SingleAction}
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.stats.DeltaScan
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{FileFormat, FileIndex, HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession, SQLContext}
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.util.CaseInsensitiveStringMap

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
class RemoteDeltaLog(uuid: String, initialVersion: Long, path: Path, client: DeltaLogClient) {

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
      throw DeltaErrors.pathNotExistsException(path.toString)
    }
    val fileIndex = new RemoteTahoeLogFileIndex(spark, this, path, snapshotToUse)
    new HadoopFsRelation(
      fileIndex,
      partitionSchema = snapshotToUse.metadata.partitionSchema,
      dataSchema = SchemaUtils.dropNullTypeColumns(snapshotToUse.metadata.schema),
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
      val deltaLog = DeltaLog.forTable(SparkSession.active, tablePath)
      if (path.startsWith("s3")) {
        new DeltaLogLocalClient(deltaLog, TestResource.AWS.signer)
      } else if (path.startsWith("gs")) {
        new DeltaLogLocalClient(deltaLog, TestResource.GCP.signer)
      } else if (path.startsWith("wasb")) {
        new DeltaLogLocalClient(deltaLog, TestResource.Azure.signer)
      } else {
        new DeltaLogLocalClient(deltaLog, new DummyFileSigner)
      }
    } getOrElse {
      new DeltaLogRestClient(table.apiUrl, table.apiToken)
    }
    val tableInfo = client.getTableInfo(GetTableInfoRequest(table.uuid))
    new RemoteDeltaLog(table.uuid, tableInfo.version, new Path(tableInfo.path), client)
  }
}

class RemoteSnapshot(client: DeltaLogClient, uuid: String, val version: Long) {

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

    val files = DeltaLog.filterFileList(
      metadata.partitionSchema,
      remoteFiles,
      partitionFilters).as[AddFile].collect()

    DeltaScan(version = version, files, null, null, null)(null, null, null, null)
  }
}

class DeltaExchangeDataSource extends TableProvider with RelationProvider with DataSourceRegister {

  // TODO: I have no idea what's going on here. Implementing the DataSourceV2 TableProvider without
  // retaining RelationProvider doesn't work when creating a metastore table; Spark insists on
  // looking up the USING `format` as a V1 source, and will fail if this source only uses v2.
  // But the DSv2 methods are never actually called! What having the V2 implementation does do is
  // change the value of parameters passed to the V1 createRelation() method (!!) to include the
  // TBLPROPERTIES we need.
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = new StructType()
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    print(s"\n\nFor some reason this will never be printed\n\n")
    throw new IllegalStateException("for some reason this is never hit")
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
