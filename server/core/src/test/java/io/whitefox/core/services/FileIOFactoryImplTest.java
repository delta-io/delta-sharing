package io.whitefox.core.services;

import io.whitefox.core.*;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

public class FileIOFactoryImplTest {
  private final FileIOFactoryImpl factory = new FileIOFactoryImpl();

  @Test
  void buildS3FileIO() {
    var storage = new Storage(
        "name",
        Optional.empty(),
        new Principal("Mr. Fox"),
        StorageType.S3,
        Optional.empty(),
        "s3a://my-bucket",
        0L,
        new Principal("Mr. Fox"),
        0L,
        new Principal("Mr. Fox"),
        new StorageProperties.S3Properties(
            new AwsCredentials.SimpleAwsCredentials("a", "b", "us-east1")));
    try (var fileIO = factory.newFileIO(storage)) {
      Assertions.assertInstanceOf(S3FileIO.class, fileIO);
    }
  }

  @Test
  @DisabledOnOs(OS.WINDOWS)
  void buildLocalFileIO() throws IOException {
    var storage = new Storage(
        "name",
        Optional.empty(),
        new Principal("Mr. Fox"),
        StorageType.LOCAL,
        Optional.empty(),
        "file:///tmp",
        0L,
        new Principal("Mr. Fox"),
        0L,
        new Principal("Mr. Fox"),
        new StorageProperties.LocalProperties());
    try (var fileIO = factory.newFileIO(storage)) {
      Assertions.assertInstanceOf(HadoopFileIO.class, fileIO);
      Assertions.assertDoesNotThrow(() -> {
        var of = fileIO.newOutputFile(Files.createTempFile("", "").toString());
        try (var f = of.createOrOverwrite()) {
          f.write(1);
        }
      });
    }
  }
}
