package io.whitefox.core.services.exceptions;

public class TableAlreadyExists extends AlreadyExists {
  public TableAlreadyExists() {}

  public TableAlreadyExists(String message) {
    super(message);
  }

  public TableAlreadyExists(String message, Throwable cause) {
    super(message, cause);
  }

  public TableAlreadyExists(Throwable cause) {
    super(cause);
  }

  public TableAlreadyExists(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
