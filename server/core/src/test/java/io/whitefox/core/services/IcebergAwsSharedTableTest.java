package io.whitefox.core.services;

import static io.whitefox.IcebergTestUtils.s3IcebergTableWithAwsGlueCatalog;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.wildfly.common.Assert.assertTrue;

import io.whitefox.AwsGlueTestConfig;
import io.whitefox.S3TestConfig;
import io.whitefox.core.SharedTable;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

@DisabledOnOs(OS.WINDOWS)
public class IcebergAwsSharedTableTest {

  private final IcebergTableLoader icebergTableLoader = new IcebergTableLoader(
      new IcebergCatalogHandler(new AwsGlueConfigBuilder(), new HadoopConfigBuilder()));
  private final S3TestConfig s3TestConfig = S3TestConfig.loadFromEnv();
  private final AwsGlueTestConfig awsGlueTestConfig = AwsGlueTestConfig.loadFromEnv();

  @Test
  void getTableMetadata() {
    var PTable = new SharedTable(
        "icebergtable1",
        "default",
        "share1",
        s3IcebergTableWithAwsGlueCatalog(
            s3TestConfig, awsGlueTestConfig, "test_glue_db", "icebergtable1"));
    var DTable = icebergTableLoader.loadTable(PTable);
    var metadata = DTable.getMetadata(Optional.empty());
    assertTrue(metadata.isPresent());
    assertEquals("7819530050735196523", metadata.get().id());
  }

  @Test
  void getUnknownTableMetadata() {
    var unknownPTable = new SharedTable(
        "notFound",
        "default",
        "share1",
        s3IcebergTableWithAwsGlueCatalog(
            s3TestConfig, awsGlueTestConfig, "test_glue_db", "not-found"));
    assertThrows(IllegalArgumentException.class, () -> DeltaSharedTable.of(unknownPTable));
  }
}
