package io.whitefox.api.deltasharing;

import static io.whitefox.TestingUtil.assertFails;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.wildfly.common.Assert.assertTrue;

import io.whitefox.persistence.memory.PTable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

@DisabledOnOs(OS.WINDOWS)
public class DeltaSharedTableTest {

  private static final Path deltaTablesRoot = Paths.get(".")
      .toAbsolutePath()
      .resolve("src/test/resources/delta/samples")
      .toAbsolutePath();

  private static String tablePath(String tableName) {
    return deltaTablesRoot.resolve(tableName).toUri().toString();
  }

  @Test
  void getTableVersion() throws ExecutionException, InterruptedException {
    var PTable = new PTable("delta-table", tablePath("delta-table"), "default", "share1");
    var DTable = DeltaSharedTable.of(PTable).toCompletableFuture().get();
    var version = DTable.getTableVersion(Optional.empty());
    assertEquals(Optional.of(0L), version);
  }

  @Test
  void getTableVersionNonExistingTable() throws ExecutionException, InterruptedException {
    var PTable =
        new PTable("delta-table", tablePath("delta-table-not-exists"), "default", "share1");
    var exception = assertFails(
        IllegalArgumentException.class, () -> DeltaSharedTable.of(PTable).toCompletableFuture());
    assertTrue(exception.getCause().getMessage().startsWith("Cannot find a delta table at file"));
  }

  @Test
  void getTableVersionWithTimestamp() throws ExecutionException, InterruptedException {
    var PTable = new PTable("delta-table", tablePath("delta-table"), "default", "share1");
    var DTable = DeltaSharedTable.of(PTable).toCompletableFuture().get();
    var version = DTable.getTableVersion(Optional.of("2023-09-30T10:15:30+01:00"));
    assertEquals(Optional.empty(), version);
  }

  @Test
  void getTableVersionWithFutureTimestamp() throws ExecutionException, InterruptedException {
    var PTable = new PTable("delta-table", tablePath("delta-table"), "default", "share1");
    var DTable = DeltaSharedTable.of(PTable).toCompletableFuture().get();
    var version = DTable.getTableVersion(Optional.of("2024-10-20T10:15:30+01:00"));
    assertEquals(Optional.empty(), version);
  }

  @Test
  void getTableVersionWithMalformedTimestamp() throws ExecutionException, InterruptedException {
    var PTable = new PTable("delta-table", tablePath("delta-table"), "default", "share1");
    var DTable = DeltaSharedTable.of(PTable).toCompletableFuture().get();
    assertThrows(
        DateTimeParseException.class,
        () -> DTable.getTableVersion(Optional.of("221rfewdsad10:15:30+01:00")));
  }
}
