package io.whitefox.api.deltasharing.loader;

import static io.whitefox.api.server.DeltaUtils.tablePath;
import static org.junit.jupiter.api.Assertions.*;

import io.quarkus.test.junit.QuarkusTest;
import io.whitefox.core.Table;
import io.whitefox.core.services.DeltaShareTableLoader;
import io.whitefox.core.services.DeltaSharedTable;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

@QuarkusTest
public class DeltaShareTableLoaderTest {

  private final DeltaShareTableLoader deltaShareTableLoader;

  public DeltaShareTableLoaderTest(DeltaShareTableLoader deltaShareTableLoader) {
    this.deltaShareTableLoader = deltaShareTableLoader;
  }

  @Test
  @DisabledOnOs(OS.WINDOWS)
  public void loadTable() {
    Table table = new Table("delta-table", tablePath("delta-table"), "schema", "share");
    DeltaSharedTable deltaSharedTable = deltaShareTableLoader.loadTable(table);
    assertTrue(deltaSharedTable.getTableVersion(Optional.empty()).isPresent());
    assertEquals(0, deltaSharedTable.getTableVersion(Optional.empty()).get());
  }

  @Test
  public void loadUnknownTable() {
    Table table = new Table("not-found", tablePath("not-found"), "schema", "share");
    assertThrows(IllegalArgumentException.class, () -> deltaShareTableLoader.loadTable(table));
  }
}
