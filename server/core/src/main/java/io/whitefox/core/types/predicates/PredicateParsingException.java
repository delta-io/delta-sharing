package io.whitefox.core.types.predicates;

import java.util.Arrays;

public class PredicateParsingException extends PredicateException {

  private final Exception cause;

  public PredicateParsingException(Exception cause) {
    this.cause = cause;
  }

  @Override
  public String getMessage() {
    return "Parsing of predicate failed due to: " + cause.getMessage() + "\n Stack trace: "
        + Arrays.toString(cause.getStackTrace());
  }
}
