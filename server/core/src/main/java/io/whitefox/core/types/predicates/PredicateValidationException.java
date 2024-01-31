package io.whitefox.core.types.predicates;

public class PredicateValidationException extends PredicateException {

  private final int actualNumOfChildren;
  private final Arity op;
  private final int expectedNumOfChildren;

  public PredicateValidationException(
      int actualNumOfChildren, Arity op, int expectedNumOfChildren) {
    this.actualNumOfChildren = actualNumOfChildren;
    this.op = op;
    this.expectedNumOfChildren = expectedNumOfChildren;
  }

  @Override
  public String getMessage() {
    if (op instanceof NaryOp)
      return op + " : expected at least " + expectedNumOfChildren + " children, but found "
          + actualNumOfChildren + " children";
    else
      return op + " : expected " + expectedNumOfChildren + " children, but found "
          + actualNumOfChildren + " children";
  }
}
