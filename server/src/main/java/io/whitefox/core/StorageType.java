package io.whitefox.core;

import java.util.Arrays;
import java.util.Optional;

public enum StorageType {
  S3("s3"),
  GCS("gcs"),
  ABFS("abfs");

  public final String value;

  StorageType(String value) {
    this.value = value;
  }

  public static Optional<StorageType> of(String s) {
    return Arrays.stream(values()).filter(mt -> mt.value.equalsIgnoreCase(s)).findFirst();
  }
}
