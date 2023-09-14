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

package io.delta.standalone.internal

// The following EvalResult trait/objects are used to implement logic operations
// for json predicates in the presence of Unknowns (Similar in spirit to the
// Dont-Cares or X in Karnaugh maps).
//
// This is needed to support partial filtering. An operation may encounter an error during
// evaluation (for example, a missing column in stats). In such cases, the op will return
// an EvalResultUnknown, which will be propagated up the json predicate tree and handled by
// ancestor ops.
//
// We extend the boolean logic to handle these Unknowns as follows:
//  - AND(EvalResultFalse, EvalResultUnknown) => EvalResultFalse
//  - AND(EvalResultTrue, EvalResultUnknown) => EvalResultUnknown.
//
//  - OR(EvalResultTrue, EvalResultUnknown) => EvalResultTrue
//  - OR(EvalResultFalse, EvalResultUnknown) => EvalResultUnknown
//
//  - NOT(EvalResultUnknown) => EvalResultUnknown.

// Represents an evaluation result.
sealed trait EvalResult;

// Represents the True result.
case object EvalResultTrue extends EvalResult

// Represents the False result.
case object EvalResultFalse extends EvalResult

// Represents the Unknown result.
// This can happen if we encounter an error during evaluation (for example, a
// predicate column with missing stats).
case object EvalResultUnknown extends EvalResult

object EvalResult {
  // Converts a boolean to the corresponding EvalResult
  def toEvalResult(b: Boolean): EvalResult = {
    b match {
      case true => EvalResultTrue
      case false => EvalResultFalse
    }
  }

  // Performs a NOT of the specified EvalResult.
  def not(r: EvalResult): EvalResult = {
    r match {
      case EvalResultFalse => EvalResultTrue
      case EvalResultTrue => EvalResultFalse
      case EvalResultUnknown => EvalResultUnknown
    }
  }
}

// Performs evaluation of a Json predicate op tree.
//
// The evaluation performs dynamic pruning of the parts that trigger
// errors (for example, columns with missing stats) in a safe manner.
// This is needed to support partial filtering, which ensures that we can perform
// as much data skipping as possible in the presence of unsupported/missing
// columns.
//
// The partial filtering makes use of EvalResultUnknown. Operations that encounter
// errors return EvalResultUnknown, which is then propagated up the tree
// and combined with other resutls using modified boolean logic described
// earlier. The errors are reported to the caller using the specified reportError callback.
//
// The pruning is conservative, and it guarantees that we never skip files
// that may overlap with query results.
class JsonPredicateEvaluatorV2(root: BaseOp, reportError: Option[(String) => Unit] = None) {
  import EvalResult._

  val kErrorCountThreshold = 10
  val errorCountMap = scala.collection.mutable.Map[BaseOp, Int]()

  // Evaluates this operation in the given context (a data file).
  // This is a wrapper on evalRecurse which does the main work.
  //
  // Returns true if op evaluates to true, which implies the data file could contain
  // query results and should not be skipped.
  // NOTE: If an EvalResultUnknown is returned, we map it to true, which implies
  //       that the file cannot be skipped.
  def eval(ctx: EvalContext): Boolean = {
    val evalResult = evalRecurse(root, ctx)
    evalResult match {
      case EvalResultFalse => false
      case _ => true
    }
  }

  // Same as above but returns the EvalResult instead of a Boolean.
  def evalRaw(ctx: EvalContext): EvalResult = {
    evalRecurse(root, ctx)
  }

  // Descends down the json predicate expression tree rooted at the specified op
  // and evaluates it using a post-order DFS strategy. In cases of errors, the
  // evaluation propagates an EvalResultUnknown, and then performs conservative
  // pruning at various levels to ensure we perform as much filtering as possible.
  //
  // If too many errors are seen on this op, turns off its evaluation by always
  // returning unknown.
  //
  // Returns the evaluation result, which can be true/false/unknown.
  private def evalRecurse(op: BaseOp, ctx: EvalContext): EvalResult = {
    if (tooManyErrors(op)) {
      return EvalResultUnknown
    }

    try {
      op match {
        // Evaluate boolean logic operators.
        case AndOp(children) => evalAndOp(children, ctx)
        case OrOp(children) => evalOrOp(children, ctx)
        case NotOp(children) => evalNotOp(children, ctx)

        // Evaluate comparison ops.
        case EqualOp(children) => evalEqualOp(children, ctx)
        case LessThanOp(children) => evalLessThanOp(children, ctx)
        case LessThanOrEqualOp(children) => evalLessThanOrEqualOp(children, ctx)
        case GreaterThanOp(children) => evalGreaterThanOp(children, ctx)
        case GreaterThanOrEqualOp(children) => evalGreaterThanOrEqualOp(children, ctx)

        // IsNull op.
        case IsNullOp(children) => evalIsNullOp(children, ctx)

        // Evaluate Leaf ops.
        case ColumnOp(name, valueType) => evalColumnOp(name, valueType, ctx)
        case LiteralOp(value, valueType) => evalLiteralOp(value, valueType)

        case _ =>
          throw new IllegalArgumentException("Unsupported op " + op)
      }
    } catch {
      // This can happen if the conversion itself fails due to format/type mismatches.
      // We will let our ancestor ops deal with this.
      case e: Exception =>
        recordError(e, op, ctx)
        EvalResultUnknown
    }
  }

  // Evaluates an AndOp with the following strategy:
  //   - If any of its children returns EvalResultFalse, return EvalResultFalse.
  //     It is safe to discard unknown results from other descendents in this case
  //     as the overall result is guranteed to be false.
  //   - If none of the children returns EvalResultFalse:
  //       - If any of them returns EvalResultUnknown, return EvalResultUnknown
  //         This allows higher layers to perform conservating processing as needed.
  //       - If all children return EvalResultTrue, return EvalResultTrue.
  private def evalAndOp(children: Seq[BaseOp], ctx: EvalContext): EvalResult = {
    var evalResult: EvalResult = EvalResultTrue
    children.foreach(child => {
      val childResult = evalRecurse(child, ctx)
      childResult match {
        case EvalResultFalse => return EvalResultFalse
        case EvalResultUnknown => evalResult = EvalResultUnknown
        case EvalResultTrue =>
      }
    })
    evalResult
  }

  // Evaluates an OrOp with the following strategy.
  //   - If any of its children returns EvalResultTrue, return EvalResultTrue.
  //     It is safe to discard unknown results from other descendents in this case.
  //   - If none of the children returns EvalResultTrue:
  //       - If any of them returns EvalResultUnknown, return EvalResultUnknown
  //         This allows higher layers to perform conservating processing as needed.
  //       - If all children return EvalResultFalse, return EvalResultFalse.
  private def evalOrOp(children: Seq[BaseOp], ctx: EvalContext): EvalResult = {
    var evalResult: EvalResult = EvalResultFalse
    children.foreach(child => {
      val childResult = evalRecurse(child, ctx)
      childResult match {
        case EvalResultTrue => return EvalResultTrue
        case EvalResultUnknown => evalResult = EvalResultUnknown
        case EvalResultFalse =>
      }
    })
    evalResult
  }

  // Evaluates a NotOp using the following strategy:
  //   - If its descendent returns EvalResultUnknown, return EvalResultUnknown,
  //     otherwise invert the result.
  private def evalNotOp(children: Seq[BaseOp], ctx: EvalContext): EvalResult = {
    not(evalRecurse(children(0), ctx))
  }

  // Evaluates the EqualOp.
  //   - An equality predicate maps to containment check in the range of possible
  //     values a column can take in the data file (its min/max range).
  //   - If there is an error (for example, missing stats entry for a column),
  //     we return EvalResultUnknown, which will be processed by the upstream
  //     ops in the predicate tree.
  private def evalEqualOp(children: Seq[BaseOp], ctx: EvalContext): EvalResult = {
    val (colStatsVals, literalVal, valueType) = resolveForComparison(children, ctx)
    if (colStatsVals == null || literalVal == null || valueType == null) {
      // This can happen if column does not have stats.
      // We will let our ancestor ops deal with this.
      return EvalResultUnknown
    }
    val (minColVal, maxColVal) = colStatsVals
    // Perform data type conversion and containment check.
    val result = valueType match {
      case OpDataTypes.BoolType =>
        ColumnRange.toBoolean(minColVal, maxColVal).contains(literalVal.toBoolean)
      case OpDataTypes.IntType =>
        ColumnRange.toInt(minColVal, maxColVal).contains(literalVal.toInt)
      case OpDataTypes.LongType =>
        ColumnRange.toLong(minColVal, maxColVal).contains(literalVal.toLong)
      case OpDataTypes.StringType =>
        ColumnRange(minColVal, maxColVal).contains(literalVal)
      case OpDataTypes.DateType =>
        ColumnRange.toDate(minColVal, maxColVal).contains(java.sql.Date.valueOf(literalVal))
      case OpDataTypes.FloatType =>
        ColumnRange.toFloat(minColVal, maxColVal).contains(literalVal.toFloat)
      case OpDataTypes.DoubleType =>
        ColumnRange.toDouble(minColVal, maxColVal).contains(literalVal.toDouble)
      case OpDataTypes.TimestampType =>
        ColumnRange.toTimestamp(minColVal, maxColVal).contains(TimestampUtils.parse(literalVal))
      case _ =>
        throw new IllegalArgumentException("Unsupported value type " + valueType)
    }
    toEvalResult(result)
  }

  // Evaluates the LessThanOp.
  //   - A lessThan predicate maps to "canBeLess" check in the range of possible
  //     values a column can take in the data file (its min/max range).
  //   - If there is an error (for example, missing stats entry for a column),
  //     we return EvalResultUnknown, which will be processed by the upstream
  //     ops in the predicate tree.
  private def evalLessThanOp(children: Seq[BaseOp], ctx: EvalContext): EvalResult = {
    val (colStatsVals, literalVal, valueType) = resolveForComparison(children, ctx)
    if (colStatsVals == null || literalVal == null || valueType == null) {
      // This can happen if column does not have stats.
      // We will let our ancestor ops deal with this.
      return EvalResultUnknown
    }
    val (minColVal, maxColVal) = colStatsVals

    // Perform data type conversion and match.
    val result = valueType match {
      case OpDataTypes.BoolType =>
        ColumnRange.toBoolean(minColVal, maxColVal).canBeLess(literalVal.toBoolean)
      case OpDataTypes.IntType =>
        ColumnRange.toInt(minColVal, maxColVal).canBeLess(literalVal.toInt)
      case OpDataTypes.LongType =>
        ColumnRange.toLong(minColVal, maxColVal).canBeLess(literalVal.toLong)
      case OpDataTypes.StringType =>
        ColumnRange(minColVal, maxColVal).canBeLess(literalVal)
      case OpDataTypes.DateType =>
        ColumnRange.toDate(minColVal, maxColVal).canBeLess(java.sql.Date.valueOf(literalVal))
      case OpDataTypes.FloatType =>
        ColumnRange.toFloat(minColVal, maxColVal).canBeLess(literalVal.toFloat)
      case OpDataTypes.DoubleType =>
        ColumnRange.toDouble(minColVal, maxColVal).canBeLess(literalVal.toDouble)
      case OpDataTypes.TimestampType =>
        ColumnRange.toTimestamp(minColVal, maxColVal).canBeLess(TimestampUtils.parse(literalVal))
      case _ =>
        throw new IllegalArgumentException("Unsupported value type " + valueType)
    }
    toEvalResult(result)
  }

  // Evaluates the LessThanOrEqualOp, which is a union or equal and lessThan.
  private def evalLessThanOrEqualOp(children: Seq[BaseOp], ctx: EvalContext): EvalResult = {
    // Note: We return Unknown if equality returns Unknown, since lessThan will also
    //       encounter same error and return Unknown.
    val result = evalEqualOp(children, ctx)
    if (result != EvalResultFalse) {
      return result
    }

    evalLessThanOp(children, ctx)
  }

  // Evaluates the greaterThanOp.
  //   - A greaterThan predicate maps to "canBeGreater" check in the range of possible
  //     values a column can take in the data file (its min/max range).
  //   - If there is an error (for example, missing stats entry for a column),
  //     we return EvalResultUnknown, which will be processed by the upstream
  //     ops in the predicate tree.
  private def evalGreaterThanOp(children: Seq[BaseOp], ctx: EvalContext): EvalResult = {
    val (colStatsVals, literalVal, valueType) = resolveForComparison(children, ctx)
    if (colStatsVals == null || literalVal == null || valueType == null) {
      // This can happen if column does not have stats.
      // We will let our ancestor ops deal with this.
      return EvalResultUnknown
    }
    val (minColVal, maxColVal) = colStatsVals

    // Perform data type conversion and match.
    val result = valueType match {
      case OpDataTypes.BoolType =>
        ColumnRange.toBoolean(minColVal, maxColVal).canBeGreater(literalVal.toBoolean)
      case OpDataTypes.IntType =>
        ColumnRange.toInt(minColVal, maxColVal).canBeGreater(literalVal.toInt)
      case OpDataTypes.LongType =>
        ColumnRange.toLong(minColVal, maxColVal).canBeGreater(literalVal.toLong)
      case OpDataTypes.StringType =>
        ColumnRange(minColVal, maxColVal).canBeGreater(literalVal)
      case OpDataTypes.DateType =>
        ColumnRange.toDate(minColVal, maxColVal).canBeGreater(java.sql.Date.valueOf(literalVal))
      case OpDataTypes.FloatType =>
        ColumnRange.toFloat(minColVal, maxColVal).canBeGreater(literalVal.toFloat)
      case OpDataTypes.DoubleType =>
        ColumnRange.toDouble(minColVal, maxColVal).canBeGreater(literalVal.toDouble)
      case OpDataTypes.TimestampType =>
        ColumnRange.toTimestamp(minColVal, maxColVal).canBeGreater(TimestampUtils.parse(literalVal))
      case _ =>
        throw new IllegalArgumentException("Unsupported value type " + valueType)
    }
    toEvalResult(result)
  }

  // Evaluates the GreaterThanOrEqualOp, which is a union or equal and greaterThan.
  private def evalGreaterThanOrEqualOp(children: Seq[BaseOp], ctx: EvalContext): EvalResult = {
    // Note: We return Unknown if equality returns Unknown, since lessThan will also
    //       encounter same error and return Unknown.
    val result = evalEqualOp(children, ctx)
    if (result != EvalResultFalse) {
      return result
    }
    evalGreaterThanOp(children, ctx)
  }

  // Evaluates the IsNullOp.
  //
  // This is expected to be called on Column ops.
  // All other ops are considered non-null.
  private def evalIsNullOp(children: Seq[BaseOp], ctx: EvalContext): EvalResult = {
    children(0) match {
      case ColumnOp(name, valueType) =>
        if (resolveColumn(name, ctx) == null) {
          EvalResultTrue
        } else {
          EvalResultFalse
        }
      case _ => EvalResultFalse
    }
  }

  // Evaluates the ColumnOp.
  //
  // Only boolean value columns are expected to be evaluated directly.
  // Other column types are expected to be part of some comparison op.
  private def evalColumnOp(colName: String, colType: String, ctx: EvalContext): EvalResult = {
    if (colType != OpDataTypes.BoolType) {
      return EvalResultUnknown
    }
    val statsVals = resolveColumn(colName, ctx)
    if (statsVals == null) {
      return EvalResultUnknown
    }
    val boolColRange = ColumnRange.toBoolean(statsVals._1, statsVals._2)
    // We only need to check the maxVal (since true > false).
    // If the maxVal is true, the data file will contain at least one true value for this column.
    toEvalResult(boolColRange.maxVal)
  }

  // Evaluates the LiteralOp.
  //
  // Only boolean value literals are expected to be evaluated directly.
  // Other literal types are expected to be part of some comparison op.
  private def evalLiteralOp(value: String, valueType: String): EvalResult = {
    if (valueType != OpDataTypes.BoolType) {
      return EvalResultUnknown
    }
    toEvalResult(value.toBoolean)
  }

  // A helper that extracts column related information from the children of json
  // predicate comparison op (for example evalEqualOp).
  // This information is used to evalute the comparison op.
  //
  // Assumes that one of the children is a ColumnOp, and the other is a LiteralOp.
  //
  // Returns:
  //  extracted information: <columnStatsValues, literalValue, valueType>
  //
  // Note: If we encounter an error (for example, missing stats), we return null.
  //       The caller should handle these null values and take appropriate action.
  private def resolveForComparison(
      children: Seq[BaseOp],
      ctx: EvalContext): ((String, String), String, String) = {
    if (children.size != 2) {
      return (null, null, null)
    }

    val (left, right) = (children(0), children(1))
    val (columnOp, literalOp) = if (left.isInstanceOf[ColumnOp] && right.isInstanceOf[LiteralOp]) {
      (left.asInstanceOf[ColumnOp], right.asInstanceOf[LiteralOp])
    } else if (right.isInstanceOf[ColumnOp] && left.isInstanceOf[LiteralOp]) {
      (right.asInstanceOf[ColumnOp], left.asInstanceOf[LiteralOp])
    } else {
      return (null, null, null)
    }
    if (columnOp.valueType != literalOp.valueType) {
      return (null, null, null)
    }

    val statsValues = resolveColumn(columnOp.name, ctx)
    (statsValues, literalOp.value, literalOp.valueType)
  }

  // A helper that resolves a column in the specified context (data file) and
  // returns the range of possible values the column can take in the data file.
  //   - If this is a partition column, it can take only one value.
  //   - If this is a result column, it can take a range of values.
  // Returns a range in both cases.
  //   - Returns null if a range cannot be found (for example, missing stats).
  private def resolveColumn(colName: String, ctx: EvalContext): (String, String) = {
    val pVal = ctx.partitionValues.get(colName)
    if (pVal.isDefined) {
      // This is a partition column, so return a point range.
      return (pVal.get, pVal.get)
    }
    // This is a regular column, so lookup its stats.
    ctx.statsValues.getOrElse(colName, null)
  }

  // Records the error that happened during evaluation.
  //
  // The evaluation of the specified op will turn off if number of errors exceeds
  // the error threshold.
  private def recordError(e: Exception, op: BaseOp, ctx: EvalContext): Unit = {
    val count = errorCountMap.get(op).getOrElse(0) + 1
    // We will report errors on an op at most twice.
    //   - When the error happens for the first time.
    //   - When we reach the error threshold and the op is turned off.
    if (reportError.isDefined && count == 1) {
      reportError.get("failed to evaluate op " + op + " on context " + ctx + ": " + e)
    }
    if (reportError.isDefined && count == kErrorCountThreshold) {
      reportError.get("Turning off " + op + " as we reached " + count + " errors: " + e)
    }
    errorCountMap(op) = count
  }

  // Returns true if we have seen too many errors for this op.
  private def tooManyErrors(op: BaseOp): Boolean = {
    errorCountMap.get(op).getOrElse(0) >= kErrorCountThreshold
  }
}
