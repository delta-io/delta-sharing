package io.whitefox.core;

import java.util.Arrays;
import java.util.Optional;

public enum MetastoreType {
  GLUE("glue"),
  HADOOP("hadoop");

  public final String value;

  MetastoreType(String value) {
    this.value = value;
  }

  public static Optional<MetastoreType> of(String s) {
    return Arrays.stream(values()).filter(mt -> mt.value.equalsIgnoreCase(s)).findFirst();
  }
}
