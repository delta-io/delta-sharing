package io.whitefox.core.services.exceptions;

public class UnknownResponseFormat extends IllegalArgumentException {
  public UnknownResponseFormat(String message) {
    super(message);
  }
}
