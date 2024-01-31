package io.whitefox.core.types.predicates;

public class PredicateColumnEvaluationException extends PredicateException {

  private final EvalContext ctx;

  public PredicateColumnEvaluationException(EvalContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public String getMessage() {
    return "Column from your query does not exist in either partition columns or regular columns of the table."
        + "Partition columns: "
        + ctx.getPartitionValues().keySet().stream().reduce((s1, s2) -> s1 + "|" + s2) + "\n"
        + "Regular columns: "
        + ctx.getStatsValues().keySet().stream().reduce((s1, s2) -> s1 + "|" + s2) + "\n";
  }
}
