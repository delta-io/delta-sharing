package io.whitefox.core.services;

import static io.whitefox.IcebergTestUtils.icebergTableWithHadoopCatalog;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.wildfly.common.Assert.assertTrue;

import io.whitefox.core.SharedTable;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

@DisabledOnOs(OS.WINDOWS)
public class IcebergSharedTableTest {

  private final IcebergTableLoader icebergTableLoader = new IcebergTableLoader(
      new IcebergCatalogHandler(new AwsGlueConfigBuilder(), new HadoopConfigBuilder()));

  @Test
  void getTableMetadata() {
    var PTable = new SharedTable(
        "icebergtable1",
        "default",
        "share1",
        icebergTableWithHadoopCatalog("test_db", "icebergtable1"));
    var DTable = icebergTableLoader.loadTable(PTable);
    var metadata = DTable.getMetadata(Optional.empty());
    assertTrue(metadata.isPresent());
    assertEquals("3369848726892806393", metadata.get().id());
  }

  @Test
  void getUnknownTableMetadata() {
    var unknownPTable = new SharedTable(
        "notFound", "default", "share1", icebergTableWithHadoopCatalog("test_db", "not-found"));
    assertThrows(IllegalArgumentException.class, () -> DeltaSharedTable.of(unknownPTable));
  }
}
