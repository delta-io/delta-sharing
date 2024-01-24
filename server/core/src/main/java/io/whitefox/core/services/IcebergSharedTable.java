package io.whitefox.core.services;

import io.whitefox.core.Metadata;
import io.whitefox.core.ReadTableRequest;
import io.whitefox.core.ReadTableResultToBeSigned;
import io.whitefox.core.TableSchema;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;
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
    return getSnapshot(startingTimestamp).map(this::getMetadataFromSnapshot);
  }

  private Metadata getMetadataFromSnapshot(Snapshot snapshot) {
    return new Metadata(
        String.valueOf(snapshot.snapshotId()),
        Optional.of(icebergTable.name()),
        Optional.empty(),
        Metadata.Format.PARQUET,
        new TableSchema(tableSchemaConverter.convertIcebergSchemaToWhitefox(
            icebergTable.schema().asStruct())),
        icebergTable.spec().fields().stream()
            .map(PartitionField::name)
            .collect(Collectors.toList()),
        icebergTable.properties(),
        Optional.of(snapshot.sequenceNumber()),
        Optional.empty(), // size is fine to be empty
        Optional.empty() // numFiles is ok to be empty here too
        );
  }

  private Optional<Snapshot> getSnapshot(Optional<String> startingTimestamp) {
    return startingTimestamp
        .map(this::getTimestamp)
        .map(Timestamp::getTime)
        .map(icebergTable::snapshot)
        .or(() -> Optional.of(icebergTable.currentSnapshot()));
  }

  private Timestamp getTimestamp(String timestamp) {
    return new Timestamp(OffsetDateTime.parse(timestamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        .toInstant()
        .toEpochMilli());
  }

  @Override
  public Optional<Long> getTableVersion(Optional<String> startingTimestamp) {
    return Optional.of(0L);
  }

  @Override
  public ReadTableResultToBeSigned queryTable(ReadTableRequest readTableRequest) {
    throw new NotImplementedException();
  }
}
