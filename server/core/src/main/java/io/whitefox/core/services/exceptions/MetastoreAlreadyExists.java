package io.whitefox.core.services.exceptions;

import io.whitefox.annotations.SkipCoverageGenerated;

@SkipCoverageGenerated
public class MetastoreAlreadyExists extends AlreadyExists {
  public MetastoreAlreadyExists() {}

  public MetastoreAlreadyExists(String message) {
    super(message);
  }

  public MetastoreAlreadyExists(String message, Throwable cause) {
    super(message, cause);
  }

  public MetastoreAlreadyExists(Throwable cause) {
    super(cause);
  }

  public MetastoreAlreadyExists(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
