package io.whitefox.core.types.predicates;

public class ExpressionNotSupportedException extends PredicateException {
  private final String expression;

  public ExpressionNotSupportedException(String expression) {
    this.expression = expression;
  }

  @Override
  public String getMessage() {
    return "Unsupported expression: " + expression.toString();
  }
}
