package io.whitefox.core.services;

import io.whitefox.core.Metadata;
import io.whitefox.core.ReadTableRequest;
import io.whitefox.core.ReadTableResultToBeSigned;
import java.util.Optional;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.iceberg.Table;

public class IcebergSharedTable implements InternalSharedTable {

  private final Table icebergTable;
  private final TableSchemaConverter tableSchemaConverter;

  private IcebergSharedTable(Table icebergTable, TableSchemaConverter tableSchemaConverter) {
    this.icebergTable = icebergTable;
    this.tableSchemaConverter = tableSchemaConverter;
  }

  public static IcebergSharedTable of(
      Table icebergTable, TableSchemaConverter tableSchemaConverter) {
    return new IcebergSharedTable(icebergTable, tableSchemaConverter);
  }

  public static IcebergSharedTable of(Table icebergTable) {
    return new IcebergSharedTable(icebergTable, new TableSchemaConverter());
  }

  public Optional<Metadata> getMetadata(Optional<String> startingTimestamp) {
    throw new NotImplementedException();
  }

  @Override
  public Optional<Long> getTableVersion(Optional<String> startingTimestamp) {
    throw new NotImplementedException();
  }

  @Override
  public ReadTableResultToBeSigned queryTable(ReadTableRequest readTableRequest) {
    throw new NotImplementedException();
  }
}
