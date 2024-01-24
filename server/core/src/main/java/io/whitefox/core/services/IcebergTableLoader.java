package io.whitefox.core.services;

import io.whitefox.core.InternalTable;
import io.whitefox.core.Metastore;
import io.whitefox.core.MetastoreType;
import io.whitefox.core.SharedTable;
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
      var tableId = getTableIdentifier(sharedTable.internalTable());
      if (metastore.type() == MetastoreType.GLUE) {
        return IcebergSharedTable.of(icebergCatalogHandler.loadTableWithGlueCatalog(
            metastore, sharedTable.internalTable().provider().storage(), tableId));
      } else if (metastore.type() == MetastoreType.HADOOP) {
        return IcebergSharedTable.of(icebergCatalogHandler.loadTableWithHadoopCatalog(
            metastore, sharedTable.internalTable().provider().storage(), tableId));
      } else {
        throw new RuntimeException(String.format(
            "Metastore type: [%s] not compatible with Iceberg table", metastore.type()));
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
