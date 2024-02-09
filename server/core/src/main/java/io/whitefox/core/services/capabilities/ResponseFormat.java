package io.whitefox.core.services.capabilities;

import io.whitefox.core.services.exceptions.UnknownResponseFormat;

public enum ResponseFormat {
  parquet("parquet"),
  delta("delta");

  private final String stringRepresentation;

  public String stringRepresentation() {
    return stringRepresentation;
  }

  ResponseFormat(String str) {
    stringRepresentation = str;
  }

  /**
   * This is seen from the client perspective, i.e. a parquet client is not compatible with a delta response
   * while the other way around is compatible
   */
  public boolean isCompatibleWith(ResponseFormat other) {
    switch (this) {
      case parquet:
        switch (other) {
          case parquet:
            return true;
          case delta:
            return false;
          default:
            throw new UnknownResponseFormat("Unknown response format: " + other);
        }
      case delta:
        return true;
      default:
        throw new UnknownResponseFormat("Unknown response format: " + this);
    }
  }
}
