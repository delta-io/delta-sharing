package io.whitefox.core.services.exceptions;

public class ProviderNotFound extends NotFound {
  public ProviderNotFound() {}

  public ProviderNotFound(String message) {
    super(message);
  }

  public ProviderNotFound(String message, Throwable cause) {
    super(message, cause);
  }

  public ProviderNotFound(Throwable cause) {
    super(cause);
  }

  public ProviderNotFound(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
