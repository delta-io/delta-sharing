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
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.delta.sharing.CachedTableManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{
  And,
  Attribute,
  Cast,
  EqualTo,
  Expression,
  GenericInternalRow,
  GreaterThan,
  IsNotNull,
  LessThan,
  Literal,
  SubqueryExpression
}
import org.apache.spark.sql.execution.datasources.{FileFormat, FileIndex, HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.types.{
  DataType,
  DateType,
  IntegerType,
  LongType,
  StringType,
  StructType
}

import io.delta.sharing.spark.model.{
  AddCDCFile,
  AddFile,
  AddFileForCDF,
  CDFColumnInfo,
  FileAction,
  RemoveFile
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

  // A helper function to create partition directories from the specified actions.
  protected def makePartitionDirectories(actions: Seq[FileAction]): Seq[PartitionDirectory] = {
    val timeZone = params.spark.sessionState.conf.sessionLocalTimeZone
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

  protected def prefix(level: Int): String = {
    var prefix = ""
    for (i <- 1 to level) {
      prefix = prefix + " "
    }
    prefix
  }

  protected def getStr(expr: Expression): String = {
    var n = expr.prettyName
    expr match {
      case a: Attribute =>
        n = n + ":" + a.qualifiedName
      case l: Literal =>
        n = n + n + ":" + l.dataType + ":" + l.value
      case _ =>
    }
    n
  }

  protected def printExpr(expr: Expression, level: Int = 0): Unit = {
    if (level == 0) {
      log.info("....TRAVERSAL START.....")
    }
    val pre = prefix(level)
    log.info(pre + getStr(expr))
    expr.children.map(c => {
      printExpr(c, level + 1)
    })
    if (level == 0) {
      log.info("....TRAVERSAL END.....")
    }
  }

  // protected def getNullPredicate(): Option[PartitionPredicate] = {
    // Some(PartitionPredicate(PartitionPredicateOp.ColValue, Some("null"), None, None, None))
  // }

  protected def convertDataType(d: DataType): Option[PartitionPredicateDataType.Value] = {
    d match {
      case IntegerType => Some(PartitionPredicateDataType.IntType)
      case LongType => Some(PartitionPredicateDataType.LongType)
      case StringType => Some(PartitionPredicateDataType.StringType)
      case DateType => Some(PartitionPredicateDataType.DateType)
      case _ => throw new IllegalArgumentException("Unsupported data type " + d)
    }
  }

  protected def convertToColInfo(left: Expression, right: Expression): Option[ColumnInfo] = {
    val colName: String = left match {
      case a: Attribute => a.name
      case _ => throw new IllegalArgumentException("Unsupported left expression " + left)
    }

    val (colValue: String, dataType: PartitionPredicateDataType.Value) = if (right == null) {
      (None, PartitionPredicateDataType.StringType)
    } else {
      right match {
        case l: Literal => (l.toString, convertDataType(l.dataType))
        case _ => throw new IllegalArgumentException("Unsupported right expr " + right)
      }
    }
    Some(ColumnInfo(colName, colValue, dataType))
  }

  protected def convert(expr: Expression): Option[PartitionPredicate] = {
    expr match {
      case And(left, right) =>
        Some(PartitionPredicate(
          PartitionPredicateOp.And, None, Seq(convert(left).get, convert(right).get)
        ))
      case EqualTo(left, right) =>
        Some(PartitionPredicate(
          PartitionPredicateOp.EqualTo, convertToColInfo(left, right), Seq.empty
        ))
      case LessThan(left, right) =>
        Some(PartitionPredicate(
          PartitionPredicateOp.LessThan, convertToColInfo(left, right), Seq.empty
        ))
      case GreaterThan(left, right) =>
        Some(PartitionPredicate(
          PartitionPredicateOp.GreaterThan, convertToColInfo(left, right), Seq.empty
        ))
      case IsNotNull(child) =>
        Some(PartitionPredicate(
          PartitionPredicateOp.NotEqualTo, convertToColInfo(child, null), Seq.empty
        ))
      case _ =>
        throw new IllegalArgumentException("Unsupported expression " + expr)
    }
  }

  protected def translate(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression],
      where: String) : Option[String] = {
    try {
      log.info("Abhijit " + where + ": partitionFilters" + partitionFilters)
      log.info("Abhijit " + where + ": dataFilters" + dataFilters)
      log.info("------------------  translate -------------")
      partitionFilters.map(p => printExpr(p))

      var root: Option[PartitionPredicate] = None
      partitionFilters.map(p => {
        val f = convert(p)
        root = if (root.isEmpty) {
          f
        } else {
          Some(PartitionPredicate(PartitionPredicateOp.And, None, Seq(root.get, f.get)))
        }
      })

      val res: Option[String] = if (root.isDefined) {
        val pp_json = PartitionPredicate.toJsonStr(root.get)
        log.info("pp_json=" + pp_json)
        val enc = new String(Base64.getUrlEncoder.encode(pp_json.getBytes), UTF_8)
        log.info("encoded_pp_json=" + enc)
        val dec = new String(Base64.getUrlDecoder.decode(enc), UTF_8)
        log.info("decoded_pp_json=" + dec)
        val pp = PartitionPredicate.fromJsonStr(dec)
        log.info("pp_from_decoded done.")

        Some(enc)
      } else {
        None
      }
      res
    } catch {
      case NonFatal(e) =>
        log.error("Error while parsing partition filters: " + e)
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
    val pp = translate(partitionFilters, dataFilters, "RemoteDeltaCDFFileIndexBase")
    makePartitionDirectories(params.snapshotAtAnalysis.filesForScan(
      partitionFilters ++ dataFilters,
      limitHint,
      pp,
      this
    ))
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

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    // We ignore partition filters for list files, since the delta sharing server already
    // parforms this.
    translate(partitionFilters, dataFilters, "RemoteDeltaCDFFileIndexBase")
    makePartitionDirectories(actions)
  }
}

// The index classes for CDF file types.

private[sharing] case class RemoteDeltaCDFAddFileIndex(
    override val params: RemoteDeltaFileIndexParams,
    addFiles: Seq[AddFileForCDF])
    extends RemoteDeltaCDFFileIndexBase(
      params,
      addFiles,
      CDFColumnInfo.getInternalPartitonSchemaForCDFAddRemoveFile) {}

private[sharing] case class RemoteDeltaCDCFileIndex(
    override val params: RemoteDeltaFileIndexParams,
    cdfFiles: Seq[AddCDCFile])
    extends RemoteDeltaCDFFileIndexBase(
      params,
      cdfFiles,
      CDFColumnInfo.getInternalPartitonSchemaForCDC) {}

private[sharing] case class RemoteDeltaCDFRemoveFileIndex(
    override val params: RemoteDeltaFileIndexParams,
    removeFiles: Seq[RemoveFile])
    extends RemoteDeltaCDFFileIndexBase(
      params,
      removeFiles,
      CDFColumnInfo.getInternalPartitonSchemaForCDFAddRemoveFile) {}

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
    // We ignore partition filters for list files, since the delta sharing server already
    // parforms the filters.
    translate(partitionFilters, dataFilters, "RemoteDeltaCDFFileIndexBase")
    makePartitionDirectories(addFiles)
  }
}
