package io.whitefox.core.services.exceptions;

public class StorageNotFound extends RuntimeException {
  public StorageNotFound() {}

  public StorageNotFound(String message) {
    super(message);
  }

  public StorageNotFound(String message, Throwable cause) {
    super(message, cause);
  }

  public StorageNotFound(Throwable cause) {
    super(cause);
  }

  public StorageNotFound(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
