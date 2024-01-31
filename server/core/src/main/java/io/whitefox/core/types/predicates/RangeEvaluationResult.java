package io.whitefox.core.types.predicates;

import io.whitefox.core.ColumnRange;

public class RangeEvaluationResult {

  ColumnRange columnRange;
  String value;

  public RangeEvaluationResult(ColumnRange columnRange, String value) {
    this.columnRange = columnRange;
    this.value = value;
  }

  public ColumnRange getColumnRange() {
    return columnRange;
  }

  public String getValue() {
    return value;
  }
}
