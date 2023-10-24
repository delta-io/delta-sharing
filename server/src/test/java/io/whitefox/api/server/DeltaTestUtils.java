package io.whitefox.api.server;

import java.nio.file.Path;
import java.nio.file.Paths;

public class DeltaTestUtils {

  private static final Path deltaTablesRoot = Paths.get(".")
      .toAbsolutePath()
      .resolve("src/test/resources/delta/samples")
      .toAbsolutePath();

  public static String tablePath(String tableName) {
    return deltaTablesRoot.resolve(tableName).toUri().toString();
  }
}
