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

// scalastyle:off println

package io.delta.sharing.spark

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.apache.spark.sql.catalyst.expressions.{
  And => SqlAnd,
  Attribute => SqlAttribute,
  AttributeReference => SqlAttributeReference,
  EqualTo => SqlEqualTo,
  Expression => SqlExpression,
  GreaterThan => SqlGreaterThan,
  IsNotNull => SqlIsNotNull,
  IsNull => SqlIsNull,
  LessThan => SqlLessThan,
  Literal => SqlLiteral
}
import org.apache.spark.sql.types.{
  BooleanType => SqlBooleanType,
  DataType => SqlDataType,
  DateType => SqlDateType,
  IntegerType => SqlIntegerType,
  LongType => SqlLongType,
  StringType => SqlStringType
}

import io.delta.sharing.spark.util.JsonUtils

case class EvalContext(
  partitionValues: Map[String, String]
)

object OpDataTypes {
  val BoolType = "bool"
  val IntType = "int"
  val LongType = "long"
  val StringType = "string"
  val DateType = "date"

  val supportedTypes = Set(BoolType, IntType, LongType, StringType, DateType)
}

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "op")
@JsonSubTypes(
  Array(
    new JsonSubTypes.Type(value = classOf[ColumnOp], name = "column"),
    new JsonSubTypes.Type(value = classOf[LiteralOp], name = "literal"),
    new JsonSubTypes.Type(value = classOf[EqualOp], name = "eq"),
    new JsonSubTypes.Type(value = classOf[NotEqualOp], name = "neq"),
    new JsonSubTypes.Type(value = classOf[LessThanOp], name = "lt"),
    new JsonSubTypes.Type(value = classOf[LessThanOrEqualOp], name = "lte"),
    new JsonSubTypes.Type(value = classOf[GreaterThanOp], name = "gt"),
    new JsonSubTypes.Type(value = classOf[GreaterThanOrEqualOp], name = "gte"),
    new JsonSubTypes.Type(value = classOf[AndOp], name = "and"),
    new JsonSubTypes.Type(value = classOf[OrOp], name = "or"),
    new JsonSubTypes.Type(value = classOf[NotOp], name = "not")
  )
)
trait BaseOp {
  def eval(ctx: EvalContext): Boolean
}

trait LeafOp extends BaseOp {
  def eval(ctx: EvalContext): Boolean = {
    throw new IllegalArgumentException("eval method not supported for leaf ops!")
  }

  def resolve(ctx: EvalContext): String

  def getValueType: String
}

trait NonLeafOp extends BaseOp {

  def equal(left: LeafOp, right: LeafOp, ctx: EvalContext): Boolean = {
    left.resolve(ctx) == right.resolve(ctx)
  }

  def lessThan(left: LeafOp, right: LeafOp, ctx: EvalContext): Boolean = {
    val leftVal = left.resolve(ctx)
    val rightVal = right.resolve(ctx)
    if (leftVal == null || rightVal == null) {
      true
    } else {
      left.getValueType match {
        case OpDataTypes.BoolType => leftVal.toBoolean < rightVal.toBoolean
        case OpDataTypes.IntType => leftVal.toInt < rightVal.toInt
        case OpDataTypes.LongType => leftVal.toLong < rightVal.toLong
        case OpDataTypes.StringType => leftVal < rightVal
        case OpDataTypes.DateType => leftVal < rightVal
        case _ =>
          throw new IllegalArgumentException("Unsupported type: " + left.getValueType)
      }
    }
  }
}

case class ColumnOp(val name: String, val valueType: String) extends LeafOp {
  override def resolve(ctx: EvalContext): String = {
    ctx.partitionValues.getOrElse(name, null)
  }

  override def getValueType: String = valueType
}

case class LiteralOp(val value: String, val valueType: String) extends LeafOp {
  override def resolve(ctx: EvalContext): String = {
    value
  }

  override def getValueType: String = valueType
}

case class EqualOp(val left: LeafOp, val right: LeafOp) extends NonLeafOp {
  override def eval(ctx: EvalContext): Boolean = equal(left, right, ctx)
}

case class NotEqualOp(val left: LeafOp, val right: LeafOp) extends NonLeafOp {
  override def eval(ctx: EvalContext): Boolean = !equal(left, right, ctx)
}

case class LessThanOp(val left: LeafOp, val right: LeafOp) extends NonLeafOp {
  override def eval(ctx: EvalContext): Boolean = lessThan(left, right, ctx)
}

case class LessThanOrEqualOp(val left: LeafOp, val right: LeafOp) extends NonLeafOp {
  override def eval(ctx: EvalContext): Boolean =
    lessThan(left, right, ctx) || equal(left, right, ctx)
}

case class GreaterThanOp(val left: LeafOp, val right: LeafOp) extends NonLeafOp {
  override def eval(ctx: EvalContext): Boolean =
    !lessThan(left, right, ctx) && !equal(left, right, ctx)
}

case class GreaterThanOrEqualOp(val left: LeafOp, val right: LeafOp) extends NonLeafOp {
  override def eval(ctx: EvalContext): Boolean = !lessThan(left, right, ctx)
}

case class AndOp(val children: Seq[NonLeafOp]) extends NonLeafOp {
  override def eval(ctx: EvalContext): Boolean = children.forall(c => c.eval(ctx))
}

case class OrOp(val children: Seq[NonLeafOp]) extends NonLeafOp {
  override def eval(ctx: EvalContext): Boolean = children.exists(c => c.eval(ctx))
}

case class NotOp(val children: Seq[NonLeafOp]) extends NonLeafOp {
  if (children.size != 1) {
    throw new IllegalArgumentException("Not op cannot have " + children.size + " children")
  }
  override def eval(ctx: EvalContext): Boolean = !children(0).eval(ctx)
}

object OpConversionUtils {

  def convertDataType(sqlType: SqlDataType): String = {
    sqlType match {
      case SqlBooleanType => OpDataTypes.BoolType
      case SqlIntegerType => OpDataTypes.IntType
      case SqlLongType => OpDataTypes.LongType
      case SqlStringType => OpDataTypes.StringType
      case SqlDateType => OpDataTypes.DateType
      case _ => throw new IllegalArgumentException("Unsupported data type " + sqlType)
    }
  }

  def convertAsLeaf(expr: SqlExpression): LeafOp = {
    expr match {
      // Leaf operators.
      case a: SqlAttributeReference => ColumnOp(a.name, convertDataType(a.dataType))
      case l: SqlLiteral => LiteralOp(l.toString, convertDataType(l.dataType))

      case _ => throw new IllegalArgumentException("Unsupported expression " + expr)
    }
  }

  def convert(expr: SqlExpression): Option[NonLeafOp] = {
    expr match {
      // Boolean operators.
      case SqlAnd(left, right) => Some(AndOp(Seq(convert(left).get, convert(right).get)))
      // case SqlOr(left, right) => Some(OrOp(Seq(convert(left).get, convert(right).get)))

      // Comparison operators
      case SqlEqualTo(left, right) => Some(EqualOp(convertAsLeaf(left), convertAsLeaf(right)))
      case SqlLessThan(left, right) => Some(LessThanOp(convertAsLeaf(left), convertAsLeaf(right)))
      case SqlGreaterThan(left, right) =>
        Some(GreaterThanOp(convertAsLeaf(left), convertAsLeaf(right)))
      case SqlIsNotNull(child) => Some(NotEqualOp(convertAsLeaf(child), LiteralOp(null, null)))
      case SqlIsNull(child) => Some(EqualOp(convertAsLeaf(child), LiteralOp(null, null)))

      // Unsupported expressions.
      case _ => throw new IllegalArgumentException("Unsupported expression " + expr)
    }
  }
}


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

object OpMapper {
  lazy val mapper = {
    val _mapper = new ObjectMapper with ScalaObjectMapper
    _mapper.setSerializationInclusion(Include.NON_ABSENT)
    _mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    _mapper.registerModule(DefaultScalaModule)
    _mapper
  }

  def toJson[T: Manifest](obj: T): String = {
    mapper.writeValueAsString(obj)
  }

  def fromJson[T: Manifest](json: String): T = {
    mapper.readValue[T](json)
  }
}

