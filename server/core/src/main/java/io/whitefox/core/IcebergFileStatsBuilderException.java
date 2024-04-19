package io.whitefox.core;

import java.util.Arrays;

public class IcebergFileStatsBuilderException extends RuntimeException {
  private final Exception cause;

  public IcebergFileStatsBuilderException(Exception cause) {
    this.cause = cause;
  }

  @Override
  public String getMessage() {
    return "Building of Iceberg file statistics failed due to: " + cause.getMessage()
        + "\n Stack trace: " + Arrays.toString(cause.getStackTrace());
  }
}
