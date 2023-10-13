package io.whitefox.core;

import java.util.Arrays;
import java.util.Optional;

public enum MetastoreType {
  GLUE("glue");

  public final String value;

  private MetastoreType(String value) {
    this.value = value;
  }

  public static Optional<MetastoreType> of(String s) {
    return Arrays.stream(values()).filter(mt -> mt.value.equalsIgnoreCase(s)).findFirst();
  }
}
