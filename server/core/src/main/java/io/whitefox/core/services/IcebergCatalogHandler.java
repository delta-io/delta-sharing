package io.whitefox.core.services;

import io.whitefox.core.Metastore;
import io.whitefox.core.MetastoreProperties;
import io.whitefox.core.Storage;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;

public class IcebergCatalogHandler {

  private final AwsGlueConfigBuilder awsGlueConfigBuilder;

  private final HadoopConfigBuilder hadoopConfigBuilder;

  public IcebergCatalogHandler(
      AwsGlueConfigBuilder awsGlueConfigBuilder, HadoopConfigBuilder hadoopConfigBuilder) {
    this.awsGlueConfigBuilder = awsGlueConfigBuilder;
    this.hadoopConfigBuilder = hadoopConfigBuilder;
  }

  public Table loadTableWithGlueCatalog(
      Metastore metastore, Storage storage, TableIdentifier tableIdentifier) {
    try (var catalog = new GlueCatalog()) {
      catalog.setConf(hadoopConfigBuilder.buildConfig(storage));
      catalog.initialize(
          metastore.name(),
          awsGlueConfigBuilder.buildConfig(
              (MetastoreProperties.GlueMetastoreProperties) metastore.properties()));
      return loadTable(catalog, tableIdentifier);
    } catch (IOException e) {
      throw new RuntimeException("Unexpected error when closing the Glue catalog", e);
    }
  }

  public Table loadTableWithHadoopCatalog(
      Metastore metastore, Storage storage, TableIdentifier tableIdentifier) {
    try (var catalog = new HadoopCatalog()) {
      catalog.setConf(hadoopConfigBuilder.buildConfig(storage));
      catalog.initialize(
          metastore.name(),
          Map.of(
              CatalogProperties.WAREHOUSE_LOCATION,
              ((MetastoreProperties.HadoopMetastoreProperties) metastore.properties()).location()));
      return loadTable(catalog, tableIdentifier);
    } catch (IOException e) {
      throw new RuntimeException("Unexpected error when closing the Hadoop catalog", e);
    }
  }

  private Table loadTable(BaseMetastoreCatalog catalog, TableIdentifier tableIdentifier) {
    try {
      return catalog.loadTable(tableIdentifier);
    } catch (NoSuchTableException e) {
      throw new IllegalArgumentException(String.format(
          "Cannot find iceberg table [%s] under namespace [%s]",
          tableIdentifier.name(), tableIdentifier.namespace()));
    } catch (Throwable e) {
      throw new RuntimeException(String.format(
          "Unexpected exception when loading the iceberg table [%s] under namespace [%s]",
          tableIdentifier.name(), tableIdentifier.namespace()));
    }
  }
}
