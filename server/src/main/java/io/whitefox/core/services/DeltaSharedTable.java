package io.whitefox.core.services;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.whitefox.core.*;
import io.whitefox.core.Metadata;
import io.whitefox.core.TableSchema;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;

public class DeltaSharedTable {

  private final DeltaLog deltaLog;
  private final TableSchemaConverter tableSchemaConverter;
  private final Table tableDetails;

  private DeltaSharedTable(
      DeltaLog deltaLog, TableSchemaConverter tableSchemaConverter, Table table) {
    this.deltaLog = deltaLog;
    this.tableSchemaConverter = tableSchemaConverter;
    this.tableDetails = table;
  }

  public static DeltaSharedTable of(Table table, TableSchemaConverter tableSchemaConverter) {
    var configuration = new Configuration();
    var dataPath = table.location();
    var dt = DeltaLog.forTable(configuration, dataPath);
    if (!dt.tableExists()) {
      throw new IllegalArgumentException(
          String.format("Cannot find a delta table at %s", dataPath));
    }
    return new DeltaSharedTable(dt, tableSchemaConverter, table);
  }

  public static DeltaSharedTable of(Table table) {
    return of(table, TableSchemaConverter.INSTANCE);
  }

  public Optional<Metadata> getMetadata(Optional<String> startingTimestamp) {
    return getSnapshot(startingTimestamp).map(this::metadataFromSnapshot);
  }

  private Metadata metadataFromSnapshot(Snapshot snapshot) {
    return new Metadata(
        snapshot.getMetadata().getId(),
        Optional.of(tableDetails.name()),
        Optional.ofNullable(snapshot.getMetadata().getDescription()),
        Metadata.Format.PARQUET,
        new TableSchema(tableSchemaConverter.convertDeltaSchemaToWhitefox(
            snapshot.getMetadata().getSchema())),
        snapshot.getMetadata().getPartitionColumns(),
        snapshot.getMetadata().getConfiguration(),
        Optional.of(snapshot.getVersion()),
        Optional.empty(), // size is fine to be empty
        Optional.empty() // numFiles is ok to be empty here too
        );
  }

  public Optional<Long> getTableVersion(Optional<String> startingTimestamp) {
    return getSnapshot(startingTimestamp).map(Snapshot::getVersion);
  }

  public ReadTableResultToBeSigned queryTable(ReadTableRequest readTableRequest) {
    Snapshot snapshot;
    if (readTableRequest instanceof ReadTableRequest.ReadTableCurrentVersion) {
      snapshot = deltaLog.snapshot();
    } else if (readTableRequest instanceof ReadTableRequest.ReadTableAsOfTimestamp) {
      snapshot = deltaLog.getSnapshotForTimestampAsOf(
          ((ReadTableRequest.ReadTableAsOfTimestamp) readTableRequest).timestamp());
    } else if (readTableRequest instanceof ReadTableRequest.ReadTableVersion) {
      snapshot = deltaLog.getSnapshotForVersionAsOf(
          ((ReadTableRequest.ReadTableVersion) readTableRequest).version());
    } else {
      throw new IllegalArgumentException("Unknown ReadTableRequest type: " + readTableRequest);
    }
    return new ReadTableResultToBeSigned(
        new Protocol(Optional.of(1)),
        metadataFromSnapshot(snapshot),
        snapshot.getAllFiles().stream()
            .map(f -> new TableFileToBeSigned(
                location() + "/" + f.getPath(),
                f.getSize(),
                snapshot.getVersion(),
                snapshot.getMetadata().getCreatedTime(),
                f.getStats(),
                f.getPartitionValues()))
            .collect(Collectors.toList()));
  }

  private Optional<Snapshot> getSnapshot(Optional<String> startingTimestamp) {
    return startingTimestamp
        .map(this::getTimestamp)
        .map(Timestamp::getTime)
        .map(this::getSnapshotForTimestampAsOf)
        .orElse(Optional.of(getSnapshot()));
  }

  private Snapshot getSnapshot() {
    return deltaLog.snapshot();
  }

  private Optional<Snapshot> getSnapshotForTimestampAsOf(long l) {
    try {
      return Optional.of(deltaLog.getSnapshotForTimestampAsOf(l));
    } catch (IllegalArgumentException iea) {
      return Optional.empty();
    }
  }

  private String location() {
    // remove all "/" at the end of the path
    return tableDetails.location().replaceAll("/+$", "");
  }

  private Timestamp getTimestamp(String timestamp) {
    return new Timestamp(OffsetDateTime.parse(timestamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        .toInstant()
        .toEpochMilli());
  }

  public static class DeltaShareTableFormat {
    public static final String RESPONSE_FORMAT_PARQUET = "parquet";
    public static final String RESPONSE_FORMAT_DELTA = "delta";
  }
}
