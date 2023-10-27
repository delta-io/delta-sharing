package io.whitefox.core.services.exceptions;

public class TableNotFound extends NotFound {
  public TableNotFound() {}

  public TableNotFound(String message) {
    super(message);
  }

  public TableNotFound(String message, Throwable cause) {
    super(message, cause);
  }

  public TableNotFound(Throwable cause) {
    super(cause);
  }

  public TableNotFound(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
