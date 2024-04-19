package io.whitefox.core.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.whitefox.core.*;
import org.apache.iceberg.catalog.TableIdentifier;

public class IcebergTableLoader implements TableLoader {

  private final IcebergCatalogHandler icebergCatalogHandler;

  public IcebergTableLoader(IcebergCatalogHandler icebergCatalogHandler) {
    this.icebergCatalogHandler = icebergCatalogHandler;
  }

  @Override
  public IcebergSharedTable loadTable(SharedTable sharedTable) {
    if (sharedTable.internalTable().properties() instanceof InternalTable.IcebergTableProperties) {
      var metastore = getMetastore(sharedTable.internalTable());
      var storage = sharedTable.internalTable().provider().storage();
      var tableId = getTableIdentifier(sharedTable.internalTable());
      if (metastore.type() == MetastoreType.GLUE) {
        return IcebergSharedTable.of(
            icebergCatalogHandler.loadTableWithGlueCatalog(
                metastore, sharedTable.internalTable().provider().storage(), tableId),
            sharedTable,
            new IcebergFileStatsBuilder(new ObjectMapper().writer()),
            new IcebergPartitionValuesBuilder());
      } else if (metastore.type() == MetastoreType.HADOOP) {
        return IcebergSharedTable.of(
            icebergCatalogHandler.loadTableWithHadoopCatalog(
                metastore, sharedTable.internalTable().provider().storage(), tableId),
            sharedTable,
            new IcebergFileStatsBuilder(new ObjectMapper().writer()),
            new IcebergPartitionValuesBuilder());
      } else {
        throw new RuntimeException(
            String.format("Unsupported metastore type: [%s]", metastore.type()));
      }
    } else {
      throw new IllegalArgumentException(
          String.format("%s is not an iceberg table", sharedTable.name()));
    }
  }

  private TableIdentifier getTableIdentifier(InternalTable internalTable) {
    var icebergTableProperties =
        ((InternalTable.IcebergTableProperties) internalTable.properties());
    return TableIdentifier.of(
        icebergTableProperties.databaseName(), icebergTableProperties.tableName());
  }

  private Metastore getMetastore(InternalTable internalTable) {
    return internalTable
        .provider()
        .metastore()
        .orElseThrow(() -> new RuntimeException(
            String.format("missing metastore for the iceberg table: [%s]", internalTable.name())));
  }
}
