package io.whitefox.core.types.predicates;

import io.whitefox.core.ColumnRange;
import io.whitefox.core.types.*;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.tuple.Pair;

// Only for partition values
public class EvalHelper {

  private static LeafEvaluationResult validateAndGetRange(
      ColumnOp columnChild, LiteralOp literalChild, EvalContext ctx) throws PredicateException {
    var columnRange = columnChild.evalExpectColumnRange(ctx);
    var rightVal = literalChild.evalExpectValueAndType(ctx).getSingleValue();

    return LeafEvaluationResult.createFromRange(new RangeEvaluationResult(columnRange, rightVal));
  }

  private static LeafEvaluationResult validateAndGetTypeAndValue(
      ColumnOp columnOp, LiteralOp literalOp, EvalContext ctx) throws PredicateException {
    var columnType = columnOp.evalExpectValueAndType(ctx).getValueType();
    var columnValue = columnOp.evalExpectValueAndType(ctx).getSingleValue();

    var literalType = literalOp.evalExpectValueAndType(ctx).getValueType();
    var literalValue = literalOp.evalExpectValueAndType(ctx).getSingleValue();
    // If the types don't match, it implies a malformed predicate tree.
    // We simply throw an exception, which will cause filtering to be skipped.
    if (!Objects.equals(columnType, literalType)) {
      throw new TypeMismatchException(columnType, literalType);
    }

    if (columnValue == null) {
      return validateAndGetRange(columnOp, literalOp, ctx);
    }

    // We throw an exception for nulls, which will skip filtering.
    if (literalValue == null) {
      throw new NullTypeException(columnOp, literalOp);
    }

    return LeafEvaluationResult.createFromPartitionColumn(
        new PartitionEvaluationResult(new ColumnRange(columnValue, columnType), literalValue));
  }

  private static Pair<ColumnOp, LiteralOp> arrangeChildren(List<LeafOp> children) {
    if (children.get(0) instanceof ColumnOp)
      return Pair.of((ColumnOp) children.get(0), (LiteralOp) children.get(1));
    else return Pair.of((ColumnOp) children.get(1), (LiteralOp) children.get(0));
  }

  // allows throwing an exception from a function passed as an argument
  @FunctionalInterface
  interface BiFunctionWithException<T, U, R, E extends Exception> {
    R apply(T t, U u) throws E;
  }

  static Boolean evaluate(
      List<LeafOp> children,
      EvalContext ctx,
      BiFunctionWithException<ColumnRange, String, Boolean, PredicateException> condition)
      throws PredicateException {
    var columnOp = arrangeChildren(children).getLeft();
    var literalOp = arrangeChildren(children).getRight();

    var leafEvaluationResult = validateAndGetTypeAndValue(columnOp, literalOp, ctx);

    if (leafEvaluationResult.rangeEvaluationResult.isPresent()) {
      var evaluationResult = leafEvaluationResult.rangeEvaluationResult.get();
      var columnRange = evaluationResult.getColumnRange();
      var value = evaluationResult.getValue();
      return condition.apply(columnRange, value);
    } else if (leafEvaluationResult.partitionEvaluationResult.isPresent()) {
      var evaluationResult = leafEvaluationResult.partitionEvaluationResult.get();
      var literalValue = evaluationResult.getLiteralValue();

      return condition.apply(evaluationResult.getPartitionValue(), literalValue);
    } else throw new PredicateColumnEvaluationException(ctx);
  }

  static Boolean equal(List<LeafOp> children, EvalContext ctx) throws PredicateException {
    return evaluate(children, ctx, ColumnRange::contains);
  }

  static Boolean lessThan(List<LeafOp> children, EvalContext ctx) throws PredicateException {
    return evaluate(children, ctx, ColumnRange::canBeLess);
  }

  // Validates that the specified value is in the correct format.
  // Throws an exception otherwise.
  public static void validateValue(String value, DataType valueType)
      throws TypeValidationException {
    try {
      if (BooleanType.BOOLEAN.equals(valueType)) {
        Boolean.parseBoolean(value);
      } else if (IntegerType.INTEGER.equals(valueType)) {
        Integer.parseInt(value);
      } else if (LongType.LONG.equals(valueType)) {
        Long.parseLong(value);
      } else if (DateType.DATE.equals(valueType)) {
        Date.valueOf(value);
      } else if (FloatType.FLOAT.equals(valueType)) {
        Float.parseFloat(value);
      } else if (DoubleType.DOUBLE.equals(valueType)) {
        Double.parseDouble(value);
      } else if (TimestampType.TIMESTAMP.equals(valueType)) {
        Timestamp.valueOf(value);
      } else if (StringType.STRING.equals(valueType)) {
        return;
      } else {
        throw new TypeNotSupportedException(valueType);
      }
    } catch (Exception e) {
      throw new TypeValidationException(value, valueType);
    }
  }
}
