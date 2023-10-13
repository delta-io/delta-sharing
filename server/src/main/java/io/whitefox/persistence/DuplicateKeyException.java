package io.whitefox.persistence;

public class DuplicateKeyException extends RuntimeException {
  public DuplicateKeyException(String message) {
    super(message);
  }
}
