package io.whitefox.core.services;

import io.whitefox.core.InternalTable;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TableLoaderFactoryImpl implements TableLoaderFactory {

  @Override
  public TableLoader newTableLoader(InternalTable internalTable) {
    if (internalTable.properties() instanceof InternalTable.DeltaTableProperties) {
      return new DeltaShareTableLoader();
    } else if (internalTable.properties() instanceof InternalTable.IcebergTableProperties) {
      return new IcebergTableLoader(
          new IcebergCatalogHandler(new AwsGlueConfigBuilder(), new HadoopConfigBuilder()));
    } else throw new RuntimeException(String.format("unknown table [%s]", internalTable.name()));
  }
}
