package io.whitefox.api.deltasharing.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * This class is test-only and it's needed because we can't know in advance pre-signed urls, in this way we can
 * easily run assertions on FileObjects that contain pre-signed urls ignoring the url.
 */
public class FileObjectFileWithoutPresignedUrl {

  private Map<String, String> partitionValues = new HashMap<>();
  private Long size;
  private String stats;
  private Long version;
  private Long timestamp;

  public FileObjectFileWithoutPresignedUrl partitionValues(Map<String, String> partitionValues) {
    this.partitionValues = partitionValues;
    return this;
  }

  @JsonProperty("partitionValues")
  @NotNull public Map<String, String> getPartitionValues() {
    return partitionValues;
  }

  @JsonProperty("partitionValues")
  public void setPartitionValues(Map<String, String> partitionValues) {
    this.partitionValues = partitionValues;
  }

  public FileObjectFileWithoutPresignedUrl putPartitionValuesItem(
      String key, String partitionValuesItem) {
    if (this.partitionValues == null) {
      this.partitionValues = new HashMap<>();
    }
    this.partitionValues.put(key, partitionValuesItem);
    return this;
  }

  public FileObjectFileWithoutPresignedUrl removePartitionValuesItem(String partitionValuesItem) {
    if (partitionValuesItem != null && this.partitionValues != null) {
      this.partitionValues.remove(partitionValuesItem);
    }
    return this;
  }

  public FileObjectFileWithoutPresignedUrl size(Long size) {
    this.size = size;
    return this;
  }

  @JsonProperty("size")
  @NotNull public Long getSize() {
    return size;
  }

  @JsonProperty("size")
  public void setSize(Long size) {
    this.size = size;
  }

  public FileObjectFileWithoutPresignedUrl stats(String stats) {
    this.stats = stats;
    return this;
  }

  @JsonProperty("stats")
  public String getStats() {
    return stats;
  }

  @JsonProperty("stats")
  public void setStats(String stats) {
    this.stats = stats;
  }

  public FileObjectFileWithoutPresignedUrl version(Long version) {
    this.version = version;
    return this;
  }

  @JsonProperty("version")
  public Long getVersion() {
    return version;
  }

  @JsonProperty("version")
  public void setVersion(Long version) {
    this.version = version;
  }

  public FileObjectFileWithoutPresignedUrl timestamp(Long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  @JsonProperty("timestamp")
  public Long getTimestamp() {
    return timestamp;
  }

  @JsonProperty("timestamp")
  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileObjectFileWithoutPresignedUrl fileObjectFileWithoutPresignedUrl =
        (FileObjectFileWithoutPresignedUrl) o;
    return Objects.equals(this.partitionValues, fileObjectFileWithoutPresignedUrl.partitionValues)
        && Objects.equals(this.size, fileObjectFileWithoutPresignedUrl.size)
        && Objects.equals(this.stats, fileObjectFileWithoutPresignedUrl.stats)
        && Objects.equals(this.version, fileObjectFileWithoutPresignedUrl.version)
        && Objects.equals(this.timestamp, fileObjectFileWithoutPresignedUrl.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionValues, size, stats, version, timestamp);
  }
}
