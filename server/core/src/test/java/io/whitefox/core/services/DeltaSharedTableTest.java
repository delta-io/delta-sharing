package io.whitefox.core.services;

import static io.whitefox.DeltaTestUtils.deltaTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.wildfly.common.Assert.assertTrue;

import io.whitefox.core.SharedTable;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

@DisabledOnOs(OS.WINDOWS)
public class DeltaSharedTableTest {

  @Test
  void getTableVersion() throws ExecutionException, InterruptedException {
    var PTable = new SharedTable("delta-table", "default", "share1", deltaTable("delta-table"));
    var DTable = DeltaSharedTable.of(PTable);
    var version = DTable.getTableVersion(Optional.empty());
    assertEquals(Optional.of(0L), version);
  }

  @Test
  void getTableMetadata() {
    var PTable = new SharedTable("delta-table", "default", "share1", deltaTable("delta-table"));
    var DTable = DeltaSharedTable.of(PTable);
    var metadata = DTable.getMetadata(Optional.empty());
    assertTrue(metadata.isPresent());
    assertEquals("56d48189-cdbc-44f2-9b0e-2bded4c79ed7", metadata.get().id());
  }

  @Test
  void getUnknownTableMetadata() {
    var unknownPTable = new SharedTable("notFound", "default", "share1", deltaTable("location1"));
    assertThrows(IllegalArgumentException.class, () -> DeltaSharedTable.of(unknownPTable));
  }

  @Test
  void getTableVersionNonExistingTable() throws ExecutionException, InterruptedException {
    var PTable =
        new SharedTable("delta-table", "default", "share1", deltaTable("delta-table-not-exists"));
    var exception = assertThrows(IllegalArgumentException.class, () -> DeltaSharedTable.of(PTable));
    assertTrue(exception.getMessage().startsWith("Cannot find a delta table at file"));
  }

  @Test
  void getTableVersionWithTimestamp() throws ExecutionException, InterruptedException {
    var PTable = new SharedTable("delta-table", "default", "share1", deltaTable("delta-table"));
    var DTable = DeltaSharedTable.of(PTable);
    var version = DTable.getTableVersion(Optional.of("2023-09-30T10:15:30+01:00"));
    assertEquals(Optional.empty(), version);
  }

  @Test
  void getTableVersionWithFutureTimestamp() throws ExecutionException, InterruptedException {
    var PTable = new SharedTable("delta-table", "default", "share1", deltaTable("delta-table"));
    var DTable = DeltaSharedTable.of(PTable);
    var version = DTable.getTableVersion(Optional.of("2024-10-20T10:15:30+01:00"));
    assertEquals(Optional.empty(), version);
  }

  @Test
  void getTableVersionWithMalformedTimestamp() throws ExecutionException, InterruptedException {
    var PTable = new SharedTable("delta-table", "default", "share1", deltaTable("delta-table"));
    var DTable = DeltaSharedTable.of(PTable);
    assertThrows(
        DateTimeParseException.class,
        () -> DTable.getTableVersion(Optional.of("221rfewdsad10:15:30+01:00")));
  }
}
