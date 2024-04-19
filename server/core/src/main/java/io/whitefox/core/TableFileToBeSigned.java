package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class TableFileToBeSigned {

  private final String url;
  private final long size;

  private final long version;

  private final Optional<Long> timestamp;

  private final String stats;
  private final Map<String, String> partitionValues;

  public TableFileToBeSigned(
      String url,
      long size,
      long version,
      Optional<Long> timestamp,
      String stats,
      Map<String, String> partitionValues) {
    this.url = url;
    this.size = size;
    this.version = version;
    this.timestamp = timestamp;
    this.stats = stats;
    this.partitionValues = partitionValues;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TableFileToBeSigned that = (TableFileToBeSigned) o;
    return size == that.size
        && version == that.version
        && Objects.equals(timestamp, that.timestamp)
        && Objects.equals(url, that.url)
        && Objects.equals(stats, that.stats)
        && Objects.equals(partitionValues, that.partitionValues);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(url, size, version, timestamp, stats, partitionValues);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "TableFileToBeSigned{" + "url='"
        + url + '\'' + ", size="
        + size + ", version="
        + version + ", timestamp="
        + timestamp + ", stats='"
        + stats + '\'' + ", partitionValues="
        + partitionValues + '}';
  }

  public String url() {
    return url;
  }

  public long size() {
    return size;
  }

  public long version() {
    return version;
  }

  public Optional<Long> timestamp() {
    return timestamp;
  }

  public String stats() {
    return stats;
  }

  public Map<String, String> partitionValues() {
    return partitionValues;
  }
}
