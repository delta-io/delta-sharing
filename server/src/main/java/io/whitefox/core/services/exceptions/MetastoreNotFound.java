package io.whitefox.core.services.exceptions;

public class MetastoreNotFound extends RuntimeException {
  public MetastoreNotFound() {}

  public MetastoreNotFound(String message) {
    super(message);
  }

  public MetastoreNotFound(String message, Throwable cause) {
    super(message, cause);
  }

  public MetastoreNotFound(Throwable cause) {
    super(cause);
  }

  public MetastoreNotFound(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
