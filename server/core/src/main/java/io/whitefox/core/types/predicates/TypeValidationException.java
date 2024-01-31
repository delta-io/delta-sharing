package io.whitefox.core.types.predicates;

import io.whitefox.core.types.DataType;

public class TypeValidationException extends PredicateException {

  private final String value;
  private final DataType valueType;

  public TypeValidationException(String value, DataType valueType) {
    this.value = value;
    this.valueType = valueType;
  }

  @Override
  public String getMessage() {
    return "Error validating value: " + value + " for type " + valueType;
  }
}
