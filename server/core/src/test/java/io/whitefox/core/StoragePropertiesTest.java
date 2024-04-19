package io.whitefox.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StoragePropertiesTest {
  private final StorageProperties.S3Properties s3props =
      new StorageProperties.S3Properties(new AwsCredentials.SimpleAwsCredentials("a", "b", "c"));

  @Test
  void testValidS3Properties() {
    Assertions.assertDoesNotThrow(
        () -> s3props.validateTypeAndUri(StorageType.S3, "s3://pippo-bucket"));
    Assertions.assertDoesNotThrow(
        () -> s3props.validateTypeAndUri(StorageType.S3, "s3n://pippo-bucket"));
    Assertions.assertDoesNotThrow(
        () -> s3props.validateTypeAndUri(StorageType.S3, "s3a://pippo-bucket"));
  }

  @Test
  void failIfWrongUri() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> s3props.validateTypeAndUri(StorageType.S3, "file:///pippo-bucket"));
  }

  @Test
  void failIfType() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> s3props.validateTypeAndUri(StorageType.LOCAL, "file:///pippo-bucket"));
  }
}
