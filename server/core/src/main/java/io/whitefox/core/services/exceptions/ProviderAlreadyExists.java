package io.whitefox.core.services.exceptions;

import io.whitefox.annotations.SkipCoverageGenerated;

@SkipCoverageGenerated
public class ProviderAlreadyExists extends AlreadyExists {
  public ProviderAlreadyExists() {}

  public ProviderAlreadyExists(String message) {
    super(message);
  }

  public ProviderAlreadyExists(String message, Throwable cause) {
    super(message, cause);
  }

  public ProviderAlreadyExists(Throwable cause) {
    super(cause);
  }

  public ProviderAlreadyExists(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
