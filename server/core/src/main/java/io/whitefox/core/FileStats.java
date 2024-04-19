package io.whitefox.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public class FileStats {
  // {"numRecords":1,"minValues":{"id":0},"maxValues":{"id":0},"nullCount":{"id":0}}
  @JsonProperty("numRecords")
  Long numRecords;

  @JsonProperty("minValues")
  Map<String, Object> minValues;

  @JsonProperty("maxValues")
  Map<String, Object> maxValues;

  @JsonProperty("nullCount")
  Map<String, Long> nullCount;

  public FileStats() {
    super();
  }

  public Long getNumRecords() {
    return numRecords;
  }

  public Map<String, Object> getMinValues() {
    return minValues;
  }

  public Map<String, Object> getMaxValues() {
    return maxValues;
  }

  public Map<String, Long> getNullCount() {
    return nullCount;
  }

  public FileStats(
      Long numRecords,
      Map<String, Object> minValues,
      Map<String, Object> maxValues,
      Map<String, Long> nullCount) {
    this.numRecords = numRecords;
    this.minValues = minValues;
    this.maxValues = maxValues;
    this.nullCount = nullCount;
  }
}
