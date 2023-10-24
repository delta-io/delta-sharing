package io.whitefox.core.services.exceptions;

import io.whitefox.annotations.SkipCoverageGenerated;

@SkipCoverageGenerated
public class NotFound extends RuntimeException {
  public NotFound() {}

  public NotFound(String message) {
    super(message);
  }

  public NotFound(String message, Throwable cause) {
    super(message, cause);
  }

  public NotFound(Throwable cause) {
    super(cause);
  }

  public NotFound(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
