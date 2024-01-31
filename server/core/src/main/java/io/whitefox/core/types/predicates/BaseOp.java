package io.whitefox.core.types.predicates;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.whitefox.core.types.*;
import java.util.List;
import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "op")
@JsonSubTypes({
  @JsonSubTypes.Type(
      value = LeafOp.class,
      names = {"column", "literal"}),
  @JsonSubTypes.Type(value = EqualOp.class, name = "equal"),
  @JsonSubTypes.Type(value = NotOp.class, name = "not"),
  @JsonSubTypes.Type(value = OrOp.class, name = "or"),
  @JsonSubTypes.Type(value = AndOp.class, name = "and"),
  @JsonSubTypes.Type(value = LessThanOp.class, name = "lessThan"),
  @JsonSubTypes.Type(value = LessThanOrEqualOp.class, name = "lessThanOrEqual"),
  @JsonSubTypes.Type(value = GreaterThanOp.class, name = "greaterThan"),
  @JsonSubTypes.Type(value = GreaterThanOrEqualOp.class, name = "greaterThanOrEqual")
})
public interface BaseOp {

  void validate() throws PredicateException;

  default Boolean isSupportedType(DataType valueType, EvaluatorVersion version) {
    if (version == EvaluatorVersion.V2) {
      return (valueType instanceof BooleanType
          || valueType instanceof IntegerType
          || valueType instanceof StringType
          || valueType instanceof DateType
          || valueType instanceof LongType
          || valueType instanceof TimestampType
          || valueType instanceof FloatType
          || valueType instanceof DoubleType);
    } else
      return (valueType instanceof BooleanType
          || valueType instanceof IntegerType
          || valueType instanceof StringType
          || valueType instanceof DateType
          || valueType instanceof LongType);
  }

  Object eval(EvalContext ctx) throws PredicateException;

  default Boolean evalExpectBoolean(EvalContext ctx) throws PredicateException {
    var res = eval(ctx);
    if (res instanceof Boolean) {
      return (Boolean) res;
    } else {
      throw new WrongExpectedTypeException(res, Boolean.class);
    }
  }

  List<BaseOp> getAllChildren();

  default Boolean treeDepthExceeds(Integer depth) {
    if (depth <= 0) {
      return true;
    } else {
      return getAllChildren().stream().anyMatch(c -> c.treeDepthExceeds(depth - 1));
    }
  }
}

// marker interface for operator arity used for easier exception handling
interface Arity {}
;

// Represents a unary operation.
interface UnaryOp extends Arity {
  // Validates number of children to be 1.
  default void validateChildren(List<BaseOp> children) throws PredicateException {
    if (children.size() != 1) throw new PredicateValidationException(children.size(), this, 1);
    try {
      children.get(0).validate();
    } catch (PredicateException e) {
      throw new RuntimeException(e);
    }
  }
}

interface BinaryOp extends Arity {
  // Validates number of children to be 2.
  default void validateChildren(List<BaseOp> children) throws PredicateException {
    if (children.size() != 2) throw new PredicateValidationException(children.size(), this, 2);

    // otherwise cannot throw exception in method call of lambda
    for (BaseOp c : children) {
      c.validate();
    }

    var child1 = children.get(0);
    var child2 = children.get(1);
    if (child1 instanceof LeafOp && child2 instanceof LeafOp) {
      var leftType = ((LeafOp) child1).getOpValueType();
      var rightType = ((LeafOp) child2).getOpValueType();
      if (!Objects.equals(leftType, rightType)) {
        throw new TypeMismatchException(leftType, rightType);
      }
    }
  }
}

// not used currently
interface NaryOp extends Arity {
  // Validates number of children to be at least 2.
  default void validateChildren(List<BaseOp> children) throws PredicateException {
    if (children.size() < 2) {
      throw new PredicateValidationException(children.size(), this, 2);
    }

    for (BaseOp c : children) {
      c.validate();
    }
  }
}
