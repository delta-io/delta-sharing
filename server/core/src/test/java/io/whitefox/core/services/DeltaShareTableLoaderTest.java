package io.whitefox.core.services;

import static io.whitefox.DeltaTestUtils.deltaTable;
import static org.junit.jupiter.api.Assertions.*;

import io.whitefox.core.SharedTable;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

public class DeltaShareTableLoaderTest {

  private final DeltaShareTableLoader deltaShareTableLoader = new DeltaShareTableLoader();

  @Test
  @DisabledOnOs(OS.WINDOWS)
  public void loadTable() {
    SharedTable sharedTable =
        new SharedTable("delta-table", "schema", "share", deltaTable("delta-table"));
    DeltaSharedTable deltaSharedTable = deltaShareTableLoader.loadTable(sharedTable);
    assertTrue(deltaSharedTable.getTableVersion(Optional.empty()).isPresent());
    assertEquals(0, deltaSharedTable.getTableVersion(Optional.empty()).get());
  }

  @Test
  public void loadUnknownTable() {
    SharedTable sharedTable =
        new SharedTable("not-found", "schema", "share", deltaTable("not-found"));
    assertThrows(
        IllegalArgumentException.class, () -> deltaShareTableLoader.loadTable(sharedTable));
  }
}
