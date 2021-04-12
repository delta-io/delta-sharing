package io.delta.exchange.spark

import java.net.{URI, URL}

import org.apache.spark.sql.delta._
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.delta.actions.{AddFile, Metadata, SingleAction}
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.stats.DeltaScan
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{FileFormat, FileIndex, HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}

case class RemoteDeltaTable(apiUrl: URL, apiToken: String, uuid: String)

object RemoteDeltaTable {
  def apply(path: String): RemoteDeltaTable = {
    // delta://uuid:token@apiurl
    val uri = new URI(path)
    assert(uri.getScheme == "delta")
    val Array(uuid, apiToken) = uri.getUserInfo.split(":")
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
  def apply(path: String): RemoteDeltaLog = {
    val table = RemoteDeltaTable(path)
    val localClientPath = SparkSession.active.conf.getOption("delta.exchange.localClient.path")
    val client = localClientPath.map { path =>
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
    client.getFiles(GetFilesRequest(uuid, version)).file.map { addFileJson =>
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

    val files = DeltaLog.filterFileList(
      metadata.partitionSchema,
      allFiles.toDF(),
      partitionFilters).as[AddFile].collect()

    DeltaScan(version = version, files, null, null, null)(null, null, null, null)
  }
}

class DeltaExchangeDataSource extends RelationProvider with DataSourceRegister {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val path = parameters.get("path")
      .getOrElse(throw new IllegalArgumentException("'path' is not specified"))
    val deltaLog = RemoteDeltaLog(path)
    deltaLog.createRelation()
  }

  override def shortName(): String = "delta-exchange"
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
