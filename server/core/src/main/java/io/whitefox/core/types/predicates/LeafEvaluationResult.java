package io.whitefox.core.types.predicates;

import java.util.Optional;

public class LeafEvaluationResult {

  Optional<RangeEvaluationResult> rangeEvaluationResult;
  Optional<PartitionEvaluationResult> partitionEvaluationResult;

  public LeafEvaluationResult(
      Optional<RangeEvaluationResult> rangeEvaluationResult,
      Optional<PartitionEvaluationResult> partitionEvaluationResult) {
    this.rangeEvaluationResult = rangeEvaluationResult;
    this.partitionEvaluationResult = partitionEvaluationResult;
  }

  public static LeafEvaluationResult createFromRange(RangeEvaluationResult rangeEvaluationResult) {
    return new LeafEvaluationResult(Optional.of(rangeEvaluationResult), Optional.empty());
  }

  public static LeafEvaluationResult createFromPartitionColumn(
      PartitionEvaluationResult partitionEvaluationResult) {
    return new LeafEvaluationResult(Optional.empty(), Optional.of(partitionEvaluationResult));
  }
}
