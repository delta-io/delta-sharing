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

import org.apache.spark.sql.catalyst.expressions.{
  And => SqlAnd,
  Attribute => SqlAttribute,
  AttributeReference => SqlAttributeReference,
  Cast => SqlCast,
  EqualNullSafe => SqlEqualNullSafe,
  EqualTo => SqlEqualTo,
  Expression => SqlExpression,
  GreaterThan => SqlGreaterThan,
  GreaterThanOrEqual => SqlGreaterThanOrEqual,
  In => SqlIn,
  IsNotNull => SqlIsNotNull,
  IsNull => SqlIsNull,
  LessThan => SqlLessThan,
  LessThanOrEqual => SqlLessThanOrEqual,
  Literal => SqlLiteral,
  Not => SqlNot,
  Or => SqlOr
}
import org.apache.spark.sql.types.{
  BooleanType => SqlBooleanType,
  DataType => SqlDataType,
  DateType => SqlDateType,
  DoubleType => SqlDoubleType,
  FloatType => SqlFloatType,
  IntegerType => SqlIntegerType,
  LongType => SqlLongType,
  StringType => SqlStringType,
  TimestampType => SqlTimestampType
}

// A set of utilites that convert SQL expressions to Json predicate Ops.
//
// We support this conversion for a subset of SQL expressions, and will
// throw an exception if the SQL expression cannot be converted. The caller
// should skip filtering in such cases.
object OpConverter {

  // Limit the number of predicates we allow in the IN op to avoid pathological cases.
  val kMaxSqlInOpSizeLimit = 20

  // Converts a set of SQL expression trees into a json predicate op.
  // We perform an implicit And across the input set at the top level.
  //
  // The method throws an exception if the conversion fails, and callers
  // should skip filtering in that case.
  @throws[IllegalArgumentException]
  def convert(expressions: Seq[SqlExpression]): Option[BaseOp] = {
    expressions.map(convertOne(_)) match {
      case Seq() => None
      case Seq(child) => Some(child)
      case children => Some(AndOp(children.toList))
    }
  }

  // Converts one SQL expression type into its corresponding Json predicate op type.
  //
  // Descends down the SQL expression tree and constucts the Json predicate tree
  // using a post-order DFS strategy.
  //
  // Throws an exception if conversion fails.
  private def convertOne(expr: SqlExpression): BaseOp = {
    // Try converting to a leaf op first, and it that fails try converting to
    // a non-leaf op.
    maybeConvertAsLeaf(expr).getOrElse(expr match {
      // Convert boolean logic operators.
      case SqlAnd(left, right) =>
        AndOp(Seq(convertOne(left), convertOne(right)))
      case SqlOr(left, right) =>
        OrOp(Seq(convertOne(left), convertOne(right)))
      case SqlNot(child) =>
        NotOp(Seq(convertOne(child)))

      // Convert comparison operators.
      case SqlEqualTo(left, right) =>
        EqualOp(Seq(convertAsLeaf(left), convertAsLeaf(right)))
      case SqlLessThan(left, right) =>
        LessThanOp(Seq(convertAsLeaf(left), convertAsLeaf(right)))
      case SqlLessThanOrEqual(left, right) =>
        LessThanOrEqualOp(Seq(convertAsLeaf(left), convertAsLeaf(right)))
      case SqlGreaterThan(left, right) =>
        GreaterThanOp(Seq(convertAsLeaf(left), convertAsLeaf(right)))
      case SqlGreaterThanOrEqual(left, right) =>
        GreaterThanOrEqualOp(Seq(convertAsLeaf(left), convertAsLeaf(right)))

      // Convert null operations.
      case SqlIsNull(child) =>
        IsNullOp(Seq(convertAsLeaf(child)))
      case SqlIsNotNull(child) =>
        NotOp(Seq(IsNullOp(Seq(convertAsLeaf(child)))))

      // Convert In operation.
      case SqlIn(value, list) =>
        // Limit the number of predicates in the op to avoid pathological cases.
        if (list.size > kMaxSqlInOpSizeLimit) {
          throw new IllegalArgumentException(
            "The In predicate exceeds max limit of " + kMaxSqlInOpSizeLimit
          )
        }
        val leafOp = convertAsLeaf(value)
        list.map(e => EqualOp(Seq(leafOp, convertAsLeaf(e)))) match {
          case Seq() =>
            throw new IllegalArgumentException("The In predicate must have at least one entry")
          case Seq(child) => child
          case children => OrOp(children)
        }

      // Convert <=> operation.
      case SqlEqualNullSafe(left, right) =>
        val leftOp = convertAsLeaf(left)
        val rightOp = convertAsLeaf(right)
        val leftIsNullOp = IsNullOp(Seq(leftOp))
        val rightIsNullOp = IsNullOp(Seq(rightOp))
        // Either both are null, or none is null and they are equal.
        OrOp(Seq(
          AndOp(Seq(leftIsNullOp, rightIsNullOp)),
          AndOp(Seq(
            NotOp(Seq(leftIsNullOp)),
            NotOp(Seq(rightIsNullOp)),
            EqualOp(Seq(leftOp, rightOp))))
        ))

      // Unsupported expressions.
      case _ =>
        throw new IllegalArgumentException("Unsupported expression during conversion " + expr)
    })
  }

  /**
   * Tries to convert the SQL expression into a json predicate leaf op.
   * If conversion fails, the function will return None.
   *
   * @param sqlTypeOpt if specified overrides the dataType specified in
   * the SQL expression. It is used to support "cast" method, which
  // forces data type convertion during evaluation.
   */
  private def maybeConvertAsLeaf(
      expr: SqlExpression,
      sqlTypeOpt: Option[SqlDataType] = None): Option[LeafOp] = {
    val sqlType = sqlTypeOpt.getOrElse(expr.dataType)

    expr match {
      case attr: SqlAttributeReference =>
        // This represents a column in partition filtering.
        Some(ColumnOp(attr.name, convertDataType(sqlType)))
      case lit: SqlLiteral =>
        // This represents a constant value to be used in comparisons.
        Some(LiteralOp(convertLiteralValue(lit, sqlType), convertDataType(sqlType)))
      case c: SqlCast =>
        // A cast operation which triggers type conversion.
        Some(convertAsLeaf(c.child, sqlTypeOpt = Some(c.dataType)))
      case _ => None
    }
  }

  // Same as maybeConvertAsLeaf, but throws an exception if conversion fails.
  private def convertAsLeaf(expr: SqlExpression, sqlTypeOpt: Option[SqlDataType] = None): LeafOp = {
    maybeConvertAsLeaf(expr, sqlTypeOpt = sqlTypeOpt).getOrElse(
      throw new IllegalArgumentException("Unsupported leaf expression " + expr)
    )
  }

  // Converts a SQL expression type into Json predicate op type.
  private def convertDataType(sqlType: SqlDataType): String = {
    sqlType match {
      case SqlBooleanType => OpDataTypes.BoolType
      case SqlIntegerType => OpDataTypes.IntType
      case SqlLongType => OpDataTypes.LongType
      case SqlStringType => OpDataTypes.StringType
      case SqlDateType => OpDataTypes.DateType
      case SqlDoubleType => OpDataTypes.DoubleType
      case SqlFloatType => OpDataTypes.FloatType
      case SqlTimestampType => OpDataTypes.TimestampType

      case _ =>
        throw new IllegalArgumentException("Unsupported data type " + sqlType)
    }
  }

  // Converts a literal value into its string representation.
  private def convertLiteralValue(lit: SqlLiteral, sqlType: SqlDataType): String = {
    sqlType match {
      case SqlTimestampType =>
        // Literal expressions store timestamp in microseconds as its value.
        // We convert it to jave.time.Instant, which formats it to ISO-8601 representation.
        java.time.Instant.ofEpochMilli(lit.value.asInstanceOf[Long] / 1000).toString()
      case _ => lit.toString
    }
  }
}
