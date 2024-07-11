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

package io.delta.kernelsharedtable

import java.sql.Date

import scala.collection.JavaConverters._
import scala.collection.mutable

import io.delta.kernel.expressions._
import io.delta.kernel.internal.util.InternalUtils
import io.delta.kernel.types._

import io.delta.sharing.server.DeltaSharingUnsupportedOperationException
import io.delta.sharing.server.common._

object PredicateConverter {


  /**
   * Converts the specified jsonPredicateHints to the corresponding Kernel predicate.
   * See [[BaseOp]] for the supported op types.
   *
   * The resulting predicate is expected to be evaluated as a boolean.
   * Note that in Delta Sharing we support some leaf expressions (Column, Literal) to be
   * directly evaluated as a boolean if it is of that type.
   */
  @throws[DeltaSharingUnsupportedOperationException]
  def convertJsonPredicateHints(
      json: String,
      partitionColumns: Seq[String],
      sizeLimit: Long,
      maxTreeDepth: Int,
      enableV2Predicate: Boolean)(implicit schema: StructType): Option[Predicate] = {
    if (partitionColumns.isEmpty && !enableV2Predicate) {
      return None
    }
    JsonPredicates
      .fromJsonWithLimits(json, sizeLimit, maxTreeDepth)
      .flatMap { op =>
        if (!enableV2Predicate) {
          val pruner = new JsonPredicatePruner(partitionColumns.toSet)
          pruner.prune(op)
        } else {
          Some(op)
        }
      }
      .map { op =>
        // Make sure the op parsed from the json is valid. This will:
        //   1. Check the number of children (e.g. binary op expects to have 2 children)
        //   2. Validate the data type of each child is supported
        //   3. Detect any type mismatch (e.g. in binary op two children should have the same type)
        op.validate(forV2 = enableV2Predicate)
        convertExpressionToPredicate(convertOpRecursively(op))
      }
  }

  // Converts a json predicate op tree to the corresponding Kernel expression tree.
  private def convertOpRecursively(op: BaseOp)(implicit schema: StructType): Expression = {
    op match {
      // Leaf op
      case ColumnOp(name, valueType) =>
        val columnDataType = getColumnDataType(name, schema)
        if (columnDataType != opTypeToDataType(valueType)) {
          // This might happen if CAST is used on the column to force data type conversion.
          // We skip filtering in this case because we're not able specify override data type
          // in Kernel Column expression.
          throw new DeltaSharingUnsupportedOperationException(
            s"Column $name has a different type than the specified type in jsonPredicateHints." +
              s" Column type = $columnDataType, specified type = $valueType.")
        }
        new Column(name)
      case LiteralOp(value, valueType) =>
        stringValueToLiteral(value, opTypeToDataType(valueType))

      // Unary op
      case NotOp(children) =>
        new Predicate(
          "NOT",
          // The child of `NOT` should always be a Predicate.
          convertExpressionToPredicate(convertOpRecursively(children.head))
        )
      case IsNullOp(children) =>
        new Predicate("IS_NULL", convertOpRecursively(children.head))

      // Binary op
      case EqualOp(children) =>
        new Predicate("=", children.map(convertOpRecursively).toList.asJava)
      case LessThanOp(children) =>
        new Predicate("<", children.map(convertOpRecursively).toList.asJava)
      case LessThanOrEqualOp(children) =>
        new Predicate("<=", children.map(convertOpRecursively).toList.asJava)
      case GreaterThanOp(children) =>
        new Predicate(">", children.map(convertOpRecursively).toList.asJava)
      case GreaterThanOrEqualOp(children) =>
        new Predicate(">=", children.map(convertOpRecursively).toList.asJava)

      // Nary op
      case AndOp(children) =>
        children.map(convertOpRecursively).reduce { (p1, p2) =>
          new And(convertExpressionToPredicate(p1), convertExpressionToPredicate(p2))
        }
      case OrOp(children) =>
        children.map(convertOpRecursively).reduce { (p1, p2) =>
          new Or(convertExpressionToPredicate(p1), convertExpressionToPredicate(p2))
        }

      case _ =>
        throw new DeltaSharingUnsupportedOperationException(s"Unsupported op $op")
    }
  }

  // Converts a Kernel Expression to a Kernel Predicate at best effort.
  //
  // Predicate is a subclass of Expression which evaluates to a boolean result.
  // In Delta Sharing, leaf expressions (Column, Literal) may be evaluated
  // to a boolean directly in some special cases.
  private def convertExpressionToPredicate(expression: Expression)(
    implicit schema: StructType): Predicate = {
    expression match {
      case p: Predicate => p
      // Delta Sharing supports column evaluation to return boolean if it is of that type.
      case c: Column if getColumnDataType(c.getNames.head, schema).isInstanceOf[BooleanType] =>
        new Predicate("=", c, Literal.ofBoolean(true))
      // Delta Sharing supports literal evaluation to return boolean if it is of that type.
      case l: Literal if l.getDataType.isInstanceOf[BooleanType] =>
        if (l.getValue.asInstanceOf[Boolean]) {
          AlwaysTrue.ALWAYS_TRUE
        } else {
          AlwaysFalse.ALWAYS_FALSE
        }
      case _ =>
        throw new DeltaSharingUnsupportedOperationException(
          s"Unsupported expression for boolean evaluation: $expression")
    }
  }

  // Converts a json predicate data type to the corresponding Kernel data type.
  private def opTypeToDataType(opType: String): DataType = {
    opType match {
      case OpDataTypes.BoolType => BooleanType.BOOLEAN
      case OpDataTypes.IntType => IntegerType.INTEGER
      case OpDataTypes.LongType => LongType.LONG
      case OpDataTypes.StringType => StringType.STRING
      case OpDataTypes.DateType => DateType.DATE
      case OpDataTypes.FloatType => FloatType.FLOAT
      case OpDataTypes.DoubleType => DoubleType.DOUBLE
      case OpDataTypes.TimestampType => TimestampType.TIMESTAMP
      case _ =>
        throw new DeltaSharingUnsupportedOperationException(s"Unsupported type: $opType")
    }
  }

  // Converts a string value to a Kernel Literal of the specified data type.
  private def stringValueToLiteral(value: String, dataType: DataType): Literal = {
    dataType match {
      case _: BooleanType => Literal.ofBoolean(value.toBoolean)
      case _: IntegerType => Literal.ofInt(value.toInt)
      case _: LongType => Literal.ofLong(value.toLong)
      case _: StringType => Literal.ofString(value)
      case _: DateType => Literal.ofDate(InternalUtils.daysSinceEpoch(Date.valueOf(value)))
      case _: FloatType => Literal.ofFloat(value.toFloat)
      case _: DoubleType => Literal.ofDouble(value.toDouble)
      case _: TimestampType =>
        Literal.ofTimestamp(InternalUtils.microsSinceEpoch(TimestampUtils.parse(value)))
      case _ =>
        throw new DeltaSharingUnsupportedOperationException(s"Unsupported type: $dataType")
    }
  }

  // Returns the data type of the specified column.
  private def getColumnDataType(columnName: String, schema: StructType): DataType = {
    if (!schema.fieldNames().contains(columnName)) {
      throw new DeltaSharingUnsupportedOperationException(
        s"Column $columnName not found in table schema.")
    }
    schema.get(columnName).getDataType
  }

  /**
   * Descends down the expression tree and collects all column names that are referred
   * by the expression into the given set.
   * @param expression The expression to collect column names from
   * @param colNameSet The set to store all column names
   */
  def collectColumnNames(expression: Expression, colNameSet: mutable.Set[String]): Unit = {
    expression match {
      case c: Column => colNameSet ++= c.getNames
      case e => e.getChildren.forEach(collectColumnNames(_, colNameSet))
    }
  }
}


object TimestampUtils {
  // The formatter we will use.
  private val formatter = java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME

  // We represent the timestamp as java.util.Timestamp in memory.
  //
  // If the timestamp is not in the correct format, this will throw an exception.
  // In the context of predicate evaluation, it will eventually turn off filtering.
  def parse(ts: String): java.sql.Timestamp = {
    new java.sql.Timestamp(java.time.OffsetDateTime.parse(ts, formatter).toInstant.toEpochMilli)
  }
}
