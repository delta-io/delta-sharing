package io.whitefox.core.types.predicates;

import io.whitefox.core.types.DataType;

public class TypeNotSupportedException extends PredicateException {
  private final DataType type;

  public TypeNotSupportedException(DataType type) {
    this.type = type;
  }

  @Override
  public String getMessage() {
    return "Unsupported type: " + type.toString();
  }
}
