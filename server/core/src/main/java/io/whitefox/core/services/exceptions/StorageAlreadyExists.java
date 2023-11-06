package io.whitefox.core.services.exceptions;

import io.whitefox.annotations.SkipCoverageGenerated;

@SkipCoverageGenerated
public class StorageAlreadyExists extends AlreadyExists {
  public StorageAlreadyExists() {}

  public StorageAlreadyExists(String message) {
    super(message);
  }

  public StorageAlreadyExists(String message, Throwable cause) {
    super(message, cause);
  }

  public StorageAlreadyExists(Throwable cause) {
    super(cause);
  }

  public StorageAlreadyExists(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
