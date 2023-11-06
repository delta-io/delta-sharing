package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class TableFile {

  private final String url;
  private final String id;
  private final long size;
  private final Optional<Long> version;
  private final Optional<Long> timestamp;
  private final Map<String, String> partitionValues;
  private final long expirationTimestamp;

  private final Optional<String> stats;

  public TableFile(
      String url,
      String id,
      long size,
      Optional<Long> version,
      Optional<Long> timestamp,
      Map<String, String> partitionValues,
      long expirationTimestamp,
      Optional<String> stats) {
    this.url = url;
    this.id = id;
    this.size = size;
    this.version = version;
    this.timestamp = timestamp;
    this.partitionValues = partitionValues;
    this.expirationTimestamp = expirationTimestamp;
    this.stats = stats;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TableFile tableFile = (TableFile) o;
    return size == tableFile.size
        && expirationTimestamp == tableFile.expirationTimestamp
        && Objects.equals(url, tableFile.url)
        && Objects.equals(id, tableFile.id)
        && Objects.equals(version, tableFile.version)
        && Objects.equals(timestamp, tableFile.timestamp)
        && Objects.equals(partitionValues, tableFile.partitionValues)
        && Objects.equals(stats, tableFile.stats);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(
        url, id, size, version, timestamp, partitionValues, expirationTimestamp, stats);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "TableFile{" + "url='"
        + url + '\'' + ", id='"
        + id + '\'' + ", size="
        + size + ", version="
        + version + ", timestamp="
        + timestamp + ", partitionValues="
        + partitionValues + ", expirationTimestamp="
        + expirationTimestamp + ", stats="
        + stats + '}';
  }

  public String url() {
    return url;
  }

  public String id() {
    return id;
  }

  public long size() {
    return size;
  }

  public Optional<Long> version() {
    return version;
  }

  public Optional<Long> timestamp() {
    return timestamp;
  }

  public Map<String, String> partitionValues() {
    return partitionValues;
  }

  public long expirationTimestamp() {
    return expirationTimestamp;
  }

  public Optional<String> stats() {
    return stats;
  }
}
