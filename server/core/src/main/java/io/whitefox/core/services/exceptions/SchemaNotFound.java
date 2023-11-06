package io.whitefox.core.services.exceptions;

public class SchemaNotFound extends NotFound {
  public SchemaNotFound() {}

  public SchemaNotFound(String message) {
    super(message);
  }

  public SchemaNotFound(String message, Throwable cause) {
    super(message, cause);
  }

  public SchemaNotFound(Throwable cause) {
    super(cause);
  }

  public SchemaNotFound(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
