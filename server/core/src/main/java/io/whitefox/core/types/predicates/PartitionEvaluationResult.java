package io.whitefox.core.types.predicates;

import io.whitefox.core.ColumnRange;

public class PartitionEvaluationResult {

  ColumnRange partitionValue;
  String literalValue;

  public PartitionEvaluationResult(ColumnRange partitionValue, String literalValue) {
    this.partitionValue = partitionValue;
    this.literalValue = literalValue;
  }

  public ColumnRange getPartitionValue() {
    return partitionValue;
  }

  public String getLiteralValue() {
    return literalValue;
  }
}
