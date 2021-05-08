package io.delta.sharing.spark

import org.apache.hadoop.fs.FileSystem

import scala.collection.JavaConverters._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.connector.catalog.{Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DeltaSharingDataSource extends TableProvider with RelationProvider with DataSourceRegister {

  DeltaSharingDataSource.reloadFileSystemsIfNeeded()

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
    properties: java.util.Map[String, String]): Table = {
    // Return a Table with no capabilities so we fall back to the v1 path.
    new Table {
      override def name(): String = s"V1FallbackTable"

      override def schema(): StructType = new StructType()

      override def capabilities(): java.util.Set[TableCapability] = Set.empty[TableCapability].asJava
    }
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse(
      "path", throw new IllegalArgumentException("'path' is not specified"))
    val deltaLog = RemoteDeltaLog(path)
    deltaLog.createRelation()
  }

  override def shortName() = "deltaSharing"
}

object DeltaSharingDataSource {
  private var reloaded = false

  def reloadFileSystemsIfNeeded(): Unit = synchronized {
    if (!reloaded) {
      reloaded = true
      val f = classOf[FileSystem].getDeclaredField("FILE_SYSTEMS_LOADED")
      f.setAccessible(true)
      f.set(null, false)
    }
  }
}