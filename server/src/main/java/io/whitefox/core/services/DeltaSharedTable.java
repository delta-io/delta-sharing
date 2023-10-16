package io.whitefox.core.services;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.Metadata;
import io.whitefox.core.Table;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;

public class DeltaSharedTable {

  private final DeltaLog deltaLog;

  private DeltaSharedTable(DeltaLog deltaLog) {
    this.deltaLog = deltaLog;
  }

  public static DeltaSharedTable of(Table table) {
    var configuration = new Configuration();
    var dataPath = Paths.get(table.location());

    var dt = DeltaLog.forTable(configuration, dataPath.toString());
    var snap = dt.update();
    if (snap.getVersion() == -1) {
      throw new IllegalArgumentException(
          String.format("Cannot find a delta table at %s", dataPath));
    }
    return new DeltaSharedTable(dt);
  }

  public Optional<Metadata> getMetadata(Optional<String> startingTimestamp) {
    return getSnapshot(startingTimestamp).map(Snapshot::getMetadata);
  }

  public Optional<Long> getTableVersion(Optional<String> startingTimestamp) {
    return getSnapshot(startingTimestamp).map(Snapshot::getVersion);
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
