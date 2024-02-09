package io.whitefox.core.services.exceptions;

public class IncompatibleTableWithClient extends RuntimeException {
  public IncompatibleTableWithClient(String message) {
    super(message);
  }
}
