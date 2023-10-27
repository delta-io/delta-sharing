package io.whitefox.api.server;

import io.whitefox.core.InternalTable;
import io.whitefox.core.Principal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class DeltaTestUtils {

  private static final Path deltaTablesRoot = Paths.get(".")
      .toAbsolutePath()
      .resolve("src/test/resources/delta/samples")
      .toAbsolutePath();

  public static String deltaTableUri(String tableName) {
    return deltaTablesRoot
        .resolve(tableName)
        .toAbsolutePath()
        .normalize()
        .toUri()
        .toString();
  }

  public static InternalTable deltaTable(String tableName) {
    return new InternalTable(
        tableName,
        Optional.empty(),
        new InternalTable.DeltaTableProperties(deltaTableUri(tableName)),
        Optional.of(0L),
        0L,
        new Principal("Mr. Fox"),
        0L,
        new Principal("Mr. Fox"),
        null // forgive me
        );
  }
}
