package io.whitefox.api.deltasharing;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.Metadata;
import io.whitefox.persistence.memory.PTable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.hadoop.conf.Configuration;

public class DeltaSharedTable {

  private final DeltaLog deltaLog;
  private final Configuration configuration;
  private final Path dataPath;

  private DeltaSharedTable(DeltaLog deltaLog, Configuration configuration, Path dataPath) {
    this.configuration = configuration;
    this.dataPath = dataPath;
    this.deltaLog = deltaLog;
  }

  public static CompletionStage<DeltaSharedTable> of(PTable table) {
    var configuration = new Configuration();
    var dataPath = Paths.get(table.location());
    return CompletableFuture.supplyAsync(() -> {
          var dt = DeltaLog.forTable(configuration, dataPath.toString());
          var snap = dt.update();
          if (snap.getVersion() == -1) {
            throw new IllegalArgumentException(
                String.format("Cannot find a delta table at %s", dataPath));
          } else {
            return dt;
          }
        })
        .thenApplyAsync(dl -> new DeltaSharedTable(dl, configuration, dataPath));
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
}
