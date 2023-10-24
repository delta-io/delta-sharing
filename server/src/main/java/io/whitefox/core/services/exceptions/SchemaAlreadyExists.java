package io.whitefox.core.services.exceptions;

import io.whitefox.annotations.SkipCoverageGenerated;

@SkipCoverageGenerated
public class SchemaAlreadyExists extends AlreadyExists {
  public SchemaAlreadyExists() {}

  public SchemaAlreadyExists(String message) {
    super(message);
  }

  public SchemaAlreadyExists(String message, Throwable cause) {
    super(message, cause);
  }

  public SchemaAlreadyExists(Throwable cause) {
    super(cause);
  }

  public SchemaAlreadyExists(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
