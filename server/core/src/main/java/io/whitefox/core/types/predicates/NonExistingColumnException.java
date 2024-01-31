package io.whitefox.core.types.predicates;

public class NonExistingColumnException extends PredicateException {
  private final String name;

  public NonExistingColumnException(String name) {
    this.name = name;
  }

  @Override
  public String getMessage() {
    return "Column " + name + " does not exist in the file statistics";
  }
}
