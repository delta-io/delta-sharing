package io.whitefox.core.types.predicates;

public class WrongExpectedTypeException extends PredicateException {

  private final Object evaluationResult;
  private final Class<?> expectedType;

  public WrongExpectedTypeException(Object evaluationResult, Class<?> expectedType) {
    this.evaluationResult = evaluationResult;
    this.expectedType = expectedType;
  }

  @Override
  public String getMessage() {
    return "Evaluation of a Root or Non-Leaf predicate is expected to be of " + expectedType
        + " type, instead got: " + evaluationResult.getClass();
  }
}
