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

import scala.collection.JavaConverters._
import java.net.URI

import org.apache.hadoop.fs.{FileStatus, Path}
import io.delta.sharing.spark.model.{AddFile, Metadata, Protocol, Table => DeltaSharingTable}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Encoder, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Cast, Expression, GenericInternalRow, Literal, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{FileFormat, FileIndex, HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.sources.BaseRelation

class RemoteDeltaLog(table: DeltaSharingTable, path: Path, client: DeltaSharingClient) {

  @volatile private var currentSnapshot: RemoteSnapshot = new RemoteSnapshot(client, table)

  protected def spark = SparkSession.active

  def snapshot: RemoteSnapshot = currentSnapshot

  def update(): Unit = synchronized {
    if (client.getTableVersion(table) != currentSnapshot.version) {
      currentSnapshot = new RemoteSnapshot(client, table)
    }
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

object RemoteDeltaLog {
  private lazy val _addFileEncoder: ExpressionEncoder[AddFile] = ExpressionEncoder[AddFile]()

  implicit def addFileEncoder: Encoder[AddFile] = {
    _addFileEncoder.copy()
  }

  def apply(path: String): RemoteDeltaLog = {
    val shapeIndex = path.lastIndexOf('#')
    if (shapeIndex < 0) {
      throw new IllegalArgumentException(s"path $path is not valid")
    }
    val profileFile = path.substring(0, shapeIndex)
    val conf = SparkSession.active.sessionState.newHadoopConf()
    val profileProvider = new DeltaSharingFileProfileProvider(conf, profileFile)
    val tableSplits = path.substring(shapeIndex + 1).split("\\.")
    if (tableSplits.length != 3) {
      throw new IllegalArgumentException(s"path $path is not valid")
    }

    val table = DeltaSharingTable(tableSplits(2), tableSplits(1), tableSplits(0))
    val sqlConf = SparkSession.active.sessionState.conf
    // This is a flag to allow us testing the server locally. Should never be used in production.
    val sslTrustAll =
      sqlConf.getConfString("spark.delta.sharing.client.sslTrustAll", "false").toBoolean
    val maxConnections = sqlConf.getConfString("spark.delta.sharing.maxConnections", "15").toInt
    val client = new DeltaSharingRestClient(profileProvider, sslTrustAll, maxConnections)
    new RemoteDeltaLog(table, new Path(path), client)
  }
}

class RemoteSnapshot(client: DeltaSharingClient, table: DeltaSharingTable) {

  protected def spark = SparkSession.active

  lazy val (metadata, protocol, version) = {
    val tableMetadata = client.getMetadata(table)
    (tableMetadata.metadata, tableMetadata.protocol, tableMetadata.version)
  }

  lazy val schema: StructType =
    Option(metadata.schemaString).map { s =>
      DataType.fromJson(s).asInstanceOf[StructType]
    }.getOrElse(StructType.apply(Nil))

  lazy val partitionSchema = new StructType(metadata.partitionColumns.map(c => schema(c)).toArray)

  def fileFormat: FileFormat = new ParquetFileFormat()

  lazy val (allFiles, sizeInBytes) = {
    val implicits = spark.implicits
    import implicits._
    val tableFiles = client.getFiles(table, Nil, None)
    checkProtocolNotChange(tableFiles.protocol)
    checkSchemaNotChange(tableFiles.metadata)
    tableFiles.files.toDS() -> tableFiles.files.map(_.size).sum
  }

  private def checkProtocolNotChange(newProtocol: Protocol): Unit = {
    if (newProtocol != protocol) {
      throw new RuntimeException(
        "The table protocol has changed since your DataFrame was created. " +
          "Please redefine your DataFrame")
    }
  }

  private def checkSchemaNotChange(newMetadata: Metadata): Unit = {
    if (newMetadata.schemaString != metadata.schemaString ||
      newMetadata.partitionColumns != metadata.partitionColumns) {
      throw new RuntimeException(
        s"""The schema or partition columns of your Delta table has changed since your
           |DataFrame was created. Please redefine your DataFrame""")
    }
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
  override def sizeInBytes: Long = deltaLog.snapshot.sizeInBytes

  override def partitionSchema: StructType = snapshotAtAnalysis.partitionSchema

  override def rootPaths: Seq[Path] = path :: Nil

  private def toDeltaPath(f: AddFile): Path = {
    DeltaSharingFileSystem.createPath(new URI(f.url), f.size)
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

          try {
            // Databricks Runtime has a different `PartitionDirectory.apply` method. We need to use
            // Java Reflection to call it.
            classOf[PartitionDirectory].getMethod("apply", classOf[InternalRow], fileStats.getClass)
              .invoke(null, new GenericInternalRow(rowValues), fileStats)
              .asInstanceOf[PartitionDirectory]
          } catch {
            case _: NoSuchMethodException =>
              // This is not in Databricks Runtime. We can call Spark's PartitionDirectory directly.
              PartitionDirectory(new GenericInternalRow(rowValues), fileStats)
          }
      }.toSeq
  }
}

object DeltaTableUtils extends PredicateHelper {

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
