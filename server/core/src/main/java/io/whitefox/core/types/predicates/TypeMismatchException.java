package io.whitefox.core.types.predicates;

import io.whitefox.core.types.DataType;

public class TypeMismatchException extends PredicateException {

  private final DataType lType;
  private final DataType rType;

  public TypeMismatchException(DataType lType, DataType rType) {
    this.lType = lType;
    this.rType = rType;
  }

  @Override
  public String getMessage() {
    return "Type are not matching between: " + lType.toString() + " and " + rType;
  }
}
