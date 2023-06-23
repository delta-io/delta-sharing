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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Cast, Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitionDirectory}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.storage.{BlockId, StorageLevel}

import io.delta.sharing.spark.filters.{BaseOp, OpConverter}
import io.delta.sharing.spark.model.{
  AddCDCFile,
  AddFile,
  AddFileForCDF,
  CDFColumnInfo,
  FileAction,
  RemoveFile,
  Table => DeltaSharingTable
}
import io.delta.sharing.spark.util.JsonUtils

private[sharing] case class RemoteDeltaFileIndexParams(
    val spark: SparkSession,
    val snapshotAtAnalysis: RemoteSnapshot,
    val profileProvider: DeltaSharingProfileProvider) {
  def path: Path = snapshotAtAnalysis.getTablePath
}

// A base class for all file indices for remote delta log.
private[sharing] abstract class RemoteDeltaFileIndexBase(
    val params: RemoteDeltaFileIndexParams) extends FileIndex with Logging {
  override def refresh(): Unit = {}

  override def sizeInBytes: Long = params.snapshotAtAnalysis.sizeInBytes

  override def partitionSchema: StructType = params.snapshotAtAnalysis.partitionSchema

  override def rootPaths: Seq[Path] = params.path :: Nil

  protected def toDeltaSharingPath(f: FileAction): Path = {
    DeltaSharingFileSystem.encode(
      params.profileProvider.getCustomTablePath(params.path.toString), f)
  }

  protected def toDeltaSharingPath(f: dsmodel.FileAction): Path = {
    DeltaSharingFileSystem.encode(
      params.profileProvider.getCustomTablePath(params.path.toString), f)
  }

  // A helper function to create partition directories from the specified actions.
  protected def makePartitionDirectories(actions: Seq[FileAction]): Seq[PartitionDirectory] = {
    val timeZone = params.spark.sessionState.conf.sessionLocalTimeZone
    // The getPartitionValuesInDF function is idempotent, and calling it multiple times does not
    // change its output.
    actions.groupBy(_.getPartitionValuesInDF()).map {
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
            toDeltaSharingPath(f))
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

  protected def getColumnFilter(partitionFilters: Seq[Expression]): Column = {
    val rewrittenFilters = DeltaTableUtils.rewritePartitionFilters(
      params.snapshotAtAnalysis.partitionSchema,
      params.spark.sessionState.conf.resolver,
      partitionFilters)
    new Column(rewrittenFilters.reduceLeftOption(And).getOrElse(Literal(true)))
  }

  // Converts the specified SQL expressions to a json predicate.
  //
  // If the conversion fails, returns a None, which will imply that we will
  // not perform json predicate based filtering.
  protected def convertToJsonPredicate(partitionFilters: Seq[Expression]) : Option[String] = {
    if (!params.spark.sessionState.conf.getConfString(
      "spark.delta.sharing.jsonPredicateHints.enabled", "true").toBoolean) {
      return None
    }
    try {
      val op = OpConverter.convert(partitionFilters)
      if (op.isDefined) {
        Some(JsonUtils.toJson[BaseOp](op.get))
      } else {
        None
      }
    } catch {
      case e: Exception =>
        log.error("Error while converting partition filters: " + e)
        None
    }
  }
}

// The index for processing files in a delta snapshot.
private[sharing] case class RemoteDeltaSnapshotFileIndex(
    override val params: RemoteDeltaFileIndexParams,
    limitHint: Option[Long]) extends RemoteDeltaFileIndexBase(params) {

  override def inputFiles: Array[String] = {
    params.snapshotAtAnalysis.filesForScan(Nil, None, None, this)
      .map(f => toDeltaSharingPath(f).toString)
      .toArray
  }

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val a = makePartitionDirectories(params.snapshotAtAnalysis.filesForScan(
      partitionFilters ++ dataFilters,
      limitHint,
      convertToJsonPredicate(partitionFilters),
      this
    ))
    // scalastyle:off println
    Console.println(s"----[linzhou]----RemoteDeltaSnapshotFileIndex.listFiles:${a}")
    a
  }
}

// A base class for all file indices for CDF.
private[sharing] abstract class RemoteDeltaCDFFileIndexBase(
    override val params: RemoteDeltaFileIndexParams,
    actions: Seq[FileAction],
    auxPartitionSchema: Map[String, DataType] = Map.empty)
    extends RemoteDeltaFileIndexBase(params) {

  override def partitionSchema: StructType = {
    DeltaTableUtils.updateSchema(params.snapshotAtAnalysis.partitionSchema, auxPartitionSchema)
  }

  override def inputFiles: Array[String] = {
    actions.map(f => toDeltaSharingPath(f).toString).toArray
  }
}

// The index classes for CDF file types.

private[sharing] case class RemoteDeltaCDFAddFileIndex(
    override val params: RemoteDeltaFileIndexParams,
    addFiles: Seq[AddFileForCDF])
    extends RemoteDeltaCDFFileIndexBase(
      params,
      addFiles,
      CDFColumnInfo.getInternalPartitonSchemaForCDFAddRemoveFile) {
  override def listFiles(
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    // Need to apply getPartitionValuesInDF to each file, to be consistent with
    // makePartitionDirectories and partitionSchema. So that partitionFilters can be correctly
    // applied.
    val updatedFiles = addFiles.map { a =>
      AddFileForCDF(a.url, a.id, a.getPartitionValuesInDF, a.size, a.version, a.timestamp, a.stats,
        a.expirationTimestamp)
    }
    val columnFilter = getColumnFilter(partitionFilters)
    val implicits = params.spark.implicits
    import implicits._
    makePartitionDirectories(updatedFiles.toDS().filter(columnFilter).as[AddFileForCDF].collect())
  }
}

private[sharing] case class RemoteDeltaCDCFileIndex(
    override val params: RemoteDeltaFileIndexParams,
    cdfFiles: Seq[AddCDCFile])
    extends RemoteDeltaCDFFileIndexBase(
      params,
      cdfFiles,
      CDFColumnInfo.getInternalPartitonSchemaForCDC) {

  override def listFiles(
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    // Need to apply getPartitionValuesInDF to each file, to be consistent with
    // makePartitionDirectories and partitionSchema. So that partitionFilters can be correctly
    // applied.
    val updatedFiles = cdfFiles.map { c =>
      AddCDCFile(c.url, c.id, c.getPartitionValuesInDF, c.size, c.version, c.timestamp,
        c.expirationTimestamp)
    }
    val columnFilter = getColumnFilter(partitionFilters)
    val implicits = params.spark.implicits
    import implicits._
    makePartitionDirectories(updatedFiles.toDS().filter(columnFilter).as[AddCDCFile].collect())
  }
}

private[sharing] case class RemoteDeltaCDFRemoveFileIndex(
    override val params: RemoteDeltaFileIndexParams,
    removeFiles: Seq[RemoveFile])
    extends RemoteDeltaCDFFileIndexBase(
      params,
      removeFiles,
      CDFColumnInfo.getInternalPartitonSchemaForCDFAddRemoveFile) {
  override def listFiles(
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    // Need to apply getPartitionValuesInDF to each file, to be consistent with
    // makePartitionDirectories and partitionSchema. So that partitionFilters can be correctly
    // applied.
    val updatedFiles = removeFiles.map { r =>
      RemoveFile(r.url, r.id, r.getPartitionValuesInDF, r.size, r.version, r.timestamp,
        r.expirationTimestamp)
    }
    val columnFilter = getColumnFilter(partitionFilters)
    val implicits = params.spark.implicits
    import implicits._
    makePartitionDirectories(updatedFiles.toDS().filter(columnFilter).as[RemoveFile].collect())
  }
}

// The index classes for batch files
private[sharing] case class RemoteDeltaBatchFileIndex(
  override val params: RemoteDeltaFileIndexParams,
  val addFiles: Seq[AddFile]) extends RemoteDeltaFileIndexBase(params) {

  override def sizeInBytes: Long = {
    addFiles.map(_.size).sum
  }

  override def inputFiles: Array[String] = {
    addFiles.map(a => toDeltaSharingPath(a).toString).toArray
  }

  override def listFiles(
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val columnFilter = getColumnFilter(partitionFilters)
    val implicits = params.spark.implicits
    import implicits._
    makePartitionDirectories(addFiles.toDS().filter(columnFilter).as[AddFile].collect())
  }
}

private[sharing] case class TmpRemoteDeltaFileIndex(
  override val params: RemoteDeltaFileIndexParams,
  versionAsOf: Option[Long],
  timestampAsOf: Option[String],
  table: DeltaSharingTable,
  client: DeltaSharingClient) extends RemoteDeltaFileIndexBase(params) {

  override def inputFiles: Array[String] = {
    throw new IllegalArgumentException("TmpRemoteDeltaFileIndex.inputFiles is not supported.")
  }

  override def listFiles(
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    // scalastyle:off println
//    Console.println(s"----[linzhou]----TmpRemoteDeltaFileIndex.listFiles:${partitionFilters}")
    val deltaTableFiles = client.getFiles(table, Nil, None, versionAsOf, timestampAsOf, None)
//    Console.println(s"----[linzhou]----deltaTableFiles:${deltaTableFiles}")
//    Console.println(s"----[linzhou]----conf:${params.spark.sessionState.conf}")
//    Console.println(s"----[linzhou]----conf-1:${getConfiguredLocalDirs(
//      SparkSession.getActiveSession.map(_.sparkContext.getConf).get).toList}")
//    Console.println(s"----[linzhou]----conf-2:${getConfiguredLocalDirs(
//      params.spark.sparkContext.getConf).toList}")

    val jsonLogBuilder = new StringBuilder()
    var jsonVersion = -1L
    deltaTableFiles.lines.foreach { line =>
      val a = JsonUtils.fromJson[dsmodel.SingleAction](line)
//      Console.println(s"----[linzhou]----line:[${line}]")
//      Console.println(s"----[linzhou]----parsed action:${a}")
      if (a.add != null) {
        jsonVersion = a.add.version
//        Console.println(s"----[linzhou]----add.id:${a.add.id}")
//        Console.println(s"----[linzhou]----add.path:${a.add.path}")
//        Console.println(s"----[linzhou]----delta sharing path:" +
//          s"${toDeltaSharingPath(a.add).toString}")
        val newAdd = dsmodel.SingleAction(
          add = a.add.copy(path = toDeltaSharingPath(a.add).toString)
        )
//        Console.println(s"----[linzhou]----new Add:[${newAdd}]")
//        Console.println(s"----[linzhou]----new Json:[${JsonUtils.toJson(newAdd)}]")
        jsonLogBuilder.append(JsonUtils.toJson(newAdd) + "\n")
      } else {
        jsonLogBuilder.append(line + "\n")
      }
    }
    val jsonLog = jsonLogBuilder.toString
//    Console.println(s"----[linzhou]----jsonVersion:${jsonVersion}")
    Console.println(s"----[linzhou]----jsonLog:${jsonLog}")

    val blockManager = SparkEnv.get.blockManager
    val contentBlockId = BlockId(s"test_randomeQuery_${table.name}_0.json")
    var blockStored = blockManager.putSingle[String](
      contentBlockId,
      jsonLog,
      StorageLevel.MEMORY_AND_DISK_SER,
      true
    )
    val sizeBlockId = BlockId(s"test_randomeQuery_cdf_table_cdf_enabled_0.json_size")
    blockStored = blockManager.putSingle[Long](
      sizeBlockId,
      jsonLog.length,
      StorageLevel.MEMORY_AND_DISK_SER,
      true
    )
    val getBack = blockManager.getSingle[String](contentBlockId)

    // TODO: create a local TahoeLogFileIndex and call listFiles of this class.
    val deltaSharingLogPath = s"delta-sharing-log:///${table.name}"
    val dL = DeltaLog.forTable(
      params.spark,
      deltaSharingLogPath,
      Map.empty[String, String]
    )
//    Console.println(s"----[linzhou]----created DeltaLog:${dL}")
    val tahoeLogFileIndex = TahoeLogFileIndex(
      params.spark,
      dL
    )
//    Console.println(s"----[linzhou]----created TahoeLogFileIndex:${tahoeLogFileIndex}")
    val a = tahoeLogFileIndex.listFiles(partitionFilters, dataFilters)
    Console.println(s"----[linzhou]----TmpRemoteDeltaFileIndex.listFiles: $a")
    a

    // throw new IllegalArgumentException(
    // "TmpRemoteDeltaFileIndex.listFiles is not fully supported.")
  }

  import org.apache.spark.SparkConf
  private def isRunningInYarnContainer(conf: SparkConf): Boolean = {
    // These environment variables are set by YARN.
    System.getenv("CONTAINER_ID") != null
  }

  /** Get the Yarn approved local directories. */
  private def getYarnLocalDirs(conf: SparkConf): String = {
    val localDirs = Option(System.getenv("LOCAL_DIRS")).getOrElse("")

    if (localDirs.isEmpty) {
      throw new Exception("Yarn Local dirs can't be empty")
    }
    localDirs
  }

  /**
   * Return the configured local directories where Spark can write files. This
   * method does not create any directories on its own, it only encapsulates the
   * logic of locating the local directories according to deployment mode.
   */
  def getConfiguredLocalDirs(conf: SparkConf): Array[String] = {
    val shuffleServiceEnabled = false
    if (isRunningInYarnContainer(conf)) {
      // If we are in yarn mode, systems can have different disk layouts so we must set it
      // to what Yarn on this system said was available. Note this assumes that Yarn has
      // created the directories already, and that they are secured so that only the
      // user has access to them.
//      Console.println(s"----[linzhou]----return-1-yarn")
      randomizeInPlace(getYarnLocalDirs(conf).split(","))
    } else if (System.getenv("SPARK_EXECUTOR_DIRS") != null) {
//      Console.println(s"----[linzhou]----return-2-executor-dirs")
      System.getenv("SPARK_EXECUTOR_DIRS").split(java.io.File.pathSeparator)
    } else if (System.getenv("SPARK_LOCAL_DIRS") != null) {
//      Console.println(s"----[linzhou]----return-3-local-dirs")
      System.getenv("SPARK_LOCAL_DIRS").split(",")
    } else if (System.getenv("MESOS_SANDBOX") != null && !shuffleServiceEnabled) {
//      Console.println(s"----[linzhou]----return-4-mesos-sandbox")
      // Mesos already creates a directory per Mesos task. Spark should use that directory
      // instead so all temporary files are automatically cleaned up when the Mesos task ends.
      // Note that we don't want this if the shuffle service is enabled because we want to
      // continue to serve shuffle files after the executors that wrote them have already exited.
      Array(System.getenv("MESOS_SANDBOX"))
    } else {
      if (System.getenv("MESOS_SANDBOX") != null && shuffleServiceEnabled) {
//        Console.println(s"----[linzhou]----return-5-just-logging")
      }
      // In non-Yarn mode (or for the driver in yarn-client mode), we cannot trust the user
      // configuration to point to a secure directory. So create a subdirectory with restricted
      // permissions under each listed directory.
//      Console.println(s"----[linzhou]----return-5-java.io.tmpdir")
      conf.get("spark.local.dir", System.getProperty("java.io.tmpdir")).split(",")
    }
  }

  /**
   * Shuffle the elements of an array into a random order, modifying the
   * original array. Returns the original array.
   */
  import java.util.Random
  def randomizeInPlace[T](arr: Array[T], rand: Random = new Random): Array[T] = {
    for (i <- (arr.length - 1) to 1 by -1) {
      val j = rand.nextInt(i + 1)
      val tmp = arr(j)
      arr(j) = arr(i)
      arr(i) = tmp
    }
    arr
  }
}
