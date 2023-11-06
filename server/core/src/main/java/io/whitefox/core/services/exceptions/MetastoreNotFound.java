package io.whitefox.core.services.exceptions;

import io.whitefox.annotations.SkipCoverageGenerated;

@SkipCoverageGenerated
public class MetastoreNotFound extends NotFound {
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
