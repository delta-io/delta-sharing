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

package io.delta.sharing.filters

import scala.collection.mutable.ListBuffer

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

/**
 * The evaluation context in which operations will be evaluated.
 *
 * @param partitionValues The partition values associated with parquet files.
 */
case class EvalContext(partitionValues: Map[String, String])

/**
 * The data types supported by the filtering operations.
 */
object OpDataTypes {
  val BoolType = "bool"
  val IntType = "int"
  val LongType = "long"
  val StringType = "string"
  val DateType = "date"
  val FloatType = "float"
  val DoubleType = "double"
  val TimestampType = "timestamp"

  val supportedTypes = Set(BoolType, IntType, LongType, StringType, DateType)
  val supportedTypesV2 = supportedTypes ++ Set(FloatType, DoubleType, TimestampType)

  // Returns true if the specified valueType is supported.
  def isSupportedType(valueType: String, forV2: Boolean): Boolean = {
    if (forV2) {
      OpDataTypes.supportedTypesV2.contains(valueType)
    } else {
      OpDataTypes.supportedTypes.contains(valueType)
    }
  }
}

/**
 * The base operation and the associated json type and sub type tagging needed
 * to convert case class hierarchy into json.
 *
 * If we add support for a new operation, we need to add an entry below
 * which will tag the operation with the given name.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "op")
@JsonSubTypes(
  Array(
    new JsonSubTypes.Type(value = classOf[ColumnOp], name = "column"),
    new JsonSubTypes.Type(value = classOf[LiteralOp], name = "literal"),
    new JsonSubTypes.Type(value = classOf[IsNullOp], name = "isNull"),
    new JsonSubTypes.Type(value = classOf[EqualOp], name = "equal"),
    new JsonSubTypes.Type(value = classOf[LessThanOp], name = "lessThan"),
    new JsonSubTypes.Type(value = classOf[LessThanOrEqualOp], name = "lessThanOrEqual"),
    new JsonSubTypes.Type(value = classOf[GreaterThanOp], name = "greaterThan"),
    new JsonSubTypes.Type(value = classOf[GreaterThanOrEqualOp], name = "greaterThanOrEqual"),
    new JsonSubTypes.Type(value = classOf[AndOp], name = "and"),
    new JsonSubTypes.Type(value = classOf[OrOp], name = "or"),
    new JsonSubTypes.Type(value = classOf[NotOp], name = "not")
  )
)
trait BaseOp {

  // Performs validations on this operation.
  //
  // The method throws an exception if any of the validations fail.
  @throws[IllegalArgumentException]
  def validate(forV2: Boolean = false): Unit

  // Evaluates this operation in the given context.
  //
  // If the method throws an exception, the caller should skip filtering for this context.
  @throws[IllegalArgumentException]
  def eval(ctx: EvalContext): Any

  // Evaluation of a non-leaf operation is expected to return a boolean.
  def evalExpectBoolean(ctx: EvalContext): Boolean = {
    eval(ctx).asInstanceOf[Boolean]
  }
}

// Represents a leaf operation.
trait LeafOp extends BaseOp {
  // Returns true if the leaf is null.
  def isNull(ctx: EvalContext): Boolean

  // Evaluation of a leaf operation is expected to return a
  // value and its type as strings.
  def evalExpectValueAndType(ctx: EvalContext): (String, String) = {
    val (value, valueType) = eval(ctx)
    (value.asInstanceOf[String], valueType.asInstanceOf[String])
  }
}

// Represents a non-leaf operation.
trait NonLeafOp extends BaseOp;

// Represents a unary operation.
trait UnaryOp {
  // Validates number of children to be 1.
  def validateChildren(children: Seq[BaseOp], forV2: Boolean = false): Unit = {
    if (children.size != 1) {
      throw new IllegalArgumentException(
          this + " : expected 1 but found " + children.size + " children"
      )
    }
    children(0).validate(forV2)
  }
}

// Represents a binary operation.
trait BinaryOp {
  // Validates number of children to be 2.
  def validateChildren(children: Seq[BaseOp], forV2: Boolean = false): Unit = {
    if (children.size != 2) {
      throw new IllegalArgumentException(
          this + " : expected 2 but found " + children.size + " children"
      )
    }
    children.map(c => c.validate(forV2))
  }
}

// Represents an operation involving two or more children.
trait NaryOp {
  // Validates number of children to be at least 2.
  def validateChildren(children: Seq[BaseOp], forV2: Boolean = false): Unit = {
    if (children.size < 2) {
      throw new IllegalArgumentException(
          this + " : expected at least 2 but found " + children.size + " children"
      )
    }
    children.map(c => c.validate(forV2))
  }
}

/**
 * Represents a column.
 *
 * @param name The name of the column as it appears in the table.
 * @param valueType The type of column's value.
 *
 * During evaluation, the name of the column is used to resolve its
 * value in the evaluation context.
 */
case class ColumnOp(val name: String, val valueType: String) extends LeafOp {
  override def validate(forV2: Boolean = false): Unit = {
    if (name == null) {
      throw new IllegalArgumentException("Name must be specified: " + this)
    }
    if (!OpDataTypes.isSupportedType(valueType, forV2)) {
      throw new IllegalArgumentException("Unsupported type: " + valueType)
    }
  }

  // Evaluates the column operation.
  // Returns the column value using the specified context, and its type.
  override def eval(ctx: EvalContext): Any = (resolve(ctx), valueType)

  // We support column evaluation to return boolean if it is of that type.
  override def evalExpectBoolean(ctx: EvalContext): Boolean = {
    if (valueType != OpDataTypes.BoolType) {
      throw new IllegalArgumentException(
        "Unsupported type for boolean evaluation: " + valueType
      )
    }
    resolve(ctx).toBoolean
  }

  // Determine if the column value is null.
  override def isNull(ctx: EvalContext): Boolean = (resolve(ctx) == null)

  // Resolve the column value using name.
  //
  // For partition filtering, we lookup the value in the specified partitionValues
  // map, which is populated from the file metadata in delta log.
  private def resolve(ctx: EvalContext): String = {
    ctx.partitionValues.getOrElse(name, null)
  }
}

/**
 * Represents a literal.
 *
 * @param value The value of this literal as a string.
 * @param valueType The type of value.
 */
case class LiteralOp(val value: String, val valueType: String) extends LeafOp {

  override def validate(forV2: Boolean = false): Unit = {
    if (value == null) {
      throw new IllegalArgumentException("Value must be specified: " + this)
    }
    if (!OpDataTypes.isSupportedType(valueType, forV2)) {
      throw new IllegalArgumentException("Unsupported type: " + valueType)
    }
  }

  // Returns the value and its type.
  override def eval(ctx: EvalContext): Any = (value, valueType)

  // Literals are always non-null.
  override def isNull(ctx: EvalContext): Boolean = false
}

/**
 * Represents a null operation on its child.
 */
case class IsNullOp(children: Seq[LeafOp]) extends NonLeafOp with UnaryOp {
  override def validate(forV2: Boolean = false): Unit = validateChildren(children, forV2)

  override def eval(ctx: EvalContext): Any = children(0).isNull(ctx)
}

/**
 * The following ops represent comparison operations on their children.
 * The (logically) supported operations are:
 *   "=", "<", "<=", ">", ">="
 *
 * We only implement "=" and "<", and represent all others in terms
 * of these two.
 *
 * @param children Expected size is 2.
 */

case class EqualOp(children: Seq[LeafOp]) extends NonLeafOp with BinaryOp {
  override def validate(forV2: Boolean = false): Unit = validateChildren(children, forV2)

  override def eval(ctx: EvalContext): Any = EvalHelper.equal(children, ctx)
}

case class LessThanOp(children: Seq[LeafOp]) extends NonLeafOp with BinaryOp {
  override def validate(forV2: Boolean = false): Unit = validateChildren(children, forV2)

  override def eval(ctx: EvalContext): Any = EvalHelper.lessThan(children, ctx)
}

case class LessThanOrEqualOp(children: Seq[LeafOp]) extends NonLeafOp with BinaryOp {
  override def validate(forV2: Boolean = false): Unit = validateChildren(children, forV2)

  override def eval(ctx: EvalContext): Any =
    EvalHelper.lessThan(children, ctx) || EvalHelper.equal(children, ctx)
}

case class GreaterThanOp(children: Seq[LeafOp]) extends NonLeafOp with BinaryOp {
  override def validate(forV2: Boolean = false): Unit = validateChildren(children, forV2)

  override def eval(ctx: EvalContext): Any =
    !EvalHelper.lessThan(children, ctx) && !EvalHelper.equal(children, ctx)
}

case class GreaterThanOrEqualOp(children: Seq[LeafOp]) extends NonLeafOp with BinaryOp {
  override def validate(forV2: Boolean = false): Unit = validateChildren(children, forV2)

  override def eval(ctx: EvalContext): Any = !EvalHelper.lessThan(children, ctx)
}

/**
 * The following ops represent boolean logic related operations.
 * The (logically) supported operations are:
 *   "&&", "||", "!"
 */

case class AndOp(val children: Seq[BaseOp]) extends NonLeafOp with NaryOp {
  override def validate(forV2: Boolean = false): Unit = validateChildren(children, forV2)

  override def eval(ctx: EvalContext): Any = {
    children.forall(c => c.evalExpectBoolean(ctx))
  }
}

case class OrOp(val children: Seq[BaseOp]) extends NonLeafOp with NaryOp {
  override def validate(forV2: Boolean = false): Unit = validateChildren(children, forV2)

  override def eval(ctx: EvalContext): Any = {
    children.exists(c => c.evalExpectBoolean(ctx))
  }
}

case class NotOp(val children: Seq[BaseOp]) extends NonLeafOp with UnaryOp {
  override def validate(forV2: Boolean = false): Unit = validateChildren(children, forV2)

  override def eval(ctx: EvalContext): Any = !children(0).evalExpectBoolean(ctx)
}

// A helper for evaluating opertions.
object EvalHelper {

  // Implements "equal" between two leaf operations.
  def equal(children: Seq[LeafOp], ctx: EvalContext): Boolean = {
    val (leftVal, leftType) = children(0).evalExpectValueAndType(ctx)
    val (rightVal, rightType) = children(1).evalExpectValueAndType(ctx)
    // If the types don't match, it implies a malformed predicate tree.
    // We simply throw an exception, which will cause filtering to be skipped.
    if (leftType != rightType) {
      throw new IllegalArgumentException(
        "Type mismatch: " + leftType + " vs " + rightType + " for " +
         children(0) + " and " + children(1)
      )
    }

    // We throw an exception for nulls, which will skip filtering.
    if (leftVal == null || rightVal == null) {
      throw new IllegalArgumentException(
        "Comparison with null is not supported: " + children(0) + " and " + children(1)
      )
      return false
    }

    // Perform data type conversion and match.
    // TODO(abhijit): For literal operations, we can optimize evaluation by caching
    //                this conversion.
    leftType match {
      case OpDataTypes.BoolType => leftVal.toBoolean == rightVal.toBoolean
      case OpDataTypes.IntType => leftVal.toInt == rightVal.toInt
      case OpDataTypes.LongType => leftVal.toLong == rightVal.toLong
      case OpDataTypes.StringType => leftVal == rightVal
      case OpDataTypes.DateType =>
        java.sql.Date.valueOf(leftVal).equals(java.sql.Date.valueOf(rightVal))
      case _ =>
        throw new IllegalArgumentException("Unsupported type: " + leftType)
    }
  }

  // Implements "less than" between two leaf operations.
  def lessThan(children: Seq[LeafOp], ctx: EvalContext): Boolean = {
    val (leftVal, leftType) = children(0).evalExpectValueAndType(ctx)
    val (rightVal, rightType) = children(1).evalExpectValueAndType(ctx)
    // If the types don't match, it implies a malformed predicate tree.
    // We simply throw an exception, which will cause filtering to be skipped.
    if (leftType != rightType) {
      throw new IllegalArgumentException(
        "Type mismatch: " + leftType + " vs " + rightType + " for " +
         children(0) + " and " + children(1)
      )
    }

    // We throw an exception for nulls, which will skip filtering.
    if (leftVal == null || rightVal == null) {
      throw new IllegalArgumentException(
        "Comparison with null is not supported: " + children(0) + " and " + children(1)
      )
    }

    // Perform data type conversion and match.
    leftType match {
      case OpDataTypes.BoolType => leftVal.toBoolean < rightVal.toBoolean
      case OpDataTypes.IntType => leftVal.toInt < rightVal.toInt
      case OpDataTypes.LongType => leftVal.toLong < rightVal.toLong
      case OpDataTypes.StringType => leftVal < rightVal
      case OpDataTypes.DateType =>
        java.sql.Date.valueOf(leftVal).before(java.sql.Date.valueOf(rightVal))
      case _ =>
        throw new IllegalArgumentException("Unsupported type: " + leftType)
    }
  }
}
