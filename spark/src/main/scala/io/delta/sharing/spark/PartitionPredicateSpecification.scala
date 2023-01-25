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

import io.delta.sharing.spark.util.JsonUtils

object PartitionPredicateOp extends Enumeration {
  val ColName, ColValue, EqualTo, NotEqualTo, LessThan, LessThanOrEqual,
    GreaterThan, GreaterThanOrEqual, And, Or = Value
}

object PartitionPredicateDataType extends Enumeration {
  val IntType, LongType, StringType, DateType = Value
}

case class ColumnInfo(
  colName: String,
  colValue: String,
  dataType: PartitionPredicateDataType.Value
) {

  def lookup(partitionValues: Map[String, String]): String = {
    partitionValues.getOrElse(colName, "")
  }

  def Equal(partitionValues: Map[String, String]): Boolean = {
    lookup(partitionValues) == colValue
  }

  def LessThan(partitionValues: Map[String, String]): Boolean = {
    dataType match {
      case PartitionPredicateDataType.IntType =>
        lookup(partitionValues).toInt < colValue.toInt
      case PartitionPredicateDataType.LongType =>
        lookup(partitionValues).toLong < colValue.toLong
      case _ =>
        lookup(partitionValues) < colValue
    }
  }
}


case class PartitionPredicate(
    op: PartitionPredicateOp.Value,
    colInfo: Option[ColumnInfo],
    children: Seq[PartitionPredicate]
) {

  def left(): PartitionPredicate = children(0)
  def right(): PartitionPredicate = children(1)

  def Equal(partitionValues: Map[String, String]): Boolean = {
    colInfo.get.Equal(partitionValues)
  }

  def LessThan(partitionValues: Map[String, String]): Boolean = {
    colInfo.get.LessThan(partitionValues)
  }

  def eval(partitionValues: Map[String, String]): Boolean = {
    op match {
      case PartitionPredicateOp.And =>
        left.eval(partitionValues) && right.eval(partitionValues)
      case PartitionPredicateOp.Or =>
        left.eval(partitionValues) || right.eval(partitionValues)

      case PartitionPredicateOp.EqualTo => Equal(partitionValues)
      case PartitionPredicateOp.LessThan => LessThan(partitionValues)

      case PartitionPredicateOp.NotEqualTo => !Equal(partitionValues)

      case PartitionPredicateOp.LessThanOrEqual =>
        LessThan(partitionValues) || Equal(partitionValues)

      case PartitionPredicateOp.GreaterThan =>
        !LessThan(partitionValues) && !Equal(partitionValues)
      case PartitionPredicateOp.GreaterThanOrEqual => !LessThan(partitionValues)

      case _ =>
        throw new IllegalArgumentException("Unsupported op: " + op)
    }
  }
}

case class PartitionPredicateOLD(
    op: PartitionPredicateOp.Value,
    tag: Option[String],
    dataType: Option[PartitionPredicateDataType.Value],
    left: Option[PartitionPredicateOLD],
    right: Option[PartitionPredicateOLD]
) {

  def getTag(partitionValues: Map[String, String]): String = {
    op match {
      case PartitionPredicateOp.ColName =>
        partitionValues.getOrElse(tag.get, "")
      case _ =>
        tag.get
    }
  }

  def Equal(partitionValues: Map[String, String]): Boolean = {
    left.get.getTag(partitionValues) == right.get.getTag(partitionValues)
  }

  def LessThan(partitionValues: Map[String, String]): Boolean = {
    val dataType = if (left.get.dataType.isDefined) {
      left.get.dataType.get
    } else if (right.get.dataType.isDefined) {
      right.get.dataType.get
    } else {
      throw new IllegalArgumentException("Data type not specified: " + this)
    }

    dataType match {
      case PartitionPredicateDataType.IntType =>
        left.get.getTag(partitionValues).toInt < right.get.getTag(partitionValues).toInt
      case PartitionPredicateDataType.LongType =>
        left.get.getTag(partitionValues).toLong < right.get.getTag(partitionValues).toLong
      case _ =>
        left.get.getTag(partitionValues) < right.get.getTag(partitionValues)
    }
  }


  def eval(partitionValues: Map[String, String]): Boolean = {
    op match {
      case PartitionPredicateOp.And =>
        left.get.eval(partitionValues) && right.get.eval(partitionValues)
      case PartitionPredicateOp.Or =>
        left.get.eval(partitionValues) || right.get.eval(partitionValues)

      case PartitionPredicateOp.EqualTo => Equal(partitionValues)
      case PartitionPredicateOp.NotEqualTo => !Equal(partitionValues)

      case PartitionPredicateOp.LessThan => LessThan(partitionValues)
      case PartitionPredicateOp.LessThanOrEqual =>
        LessThan(partitionValues) || Equal(partitionValues)

      case PartitionPredicateOp.GreaterThan =>
        !LessThan(partitionValues) && !Equal(partitionValues)
      case PartitionPredicateOp.GreaterThanOrEqual => !LessThan(partitionValues)

      case _ =>
        throw new IllegalArgumentException("Unsupported op: " + op)
    }
  }
}


object PartitionPredicate {
  // Parses the specified jsonStr string into a [[PartitionPredicate]].
  def fromJsonStr(jsonStr: String): PartitionPredicate = {
    JsonUtils.fromJson[PartitionPredicate](jsonStr)
  }

  def toJsonStr(predicate: PartitionPredicate): String = {
    JsonUtils.toJson(predicate)
  }
}
