package io.whitefox.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public class FileStats {
  // {"numRecords":1,"minValues":{"id":0},"maxValues":{"id":0},"nullCount":{"id":0}}
  @JsonProperty("numRecords")
  String numRecords;

  @JsonProperty("minValues")
  Map<String, String> minValues;

  @JsonProperty("maxValues")
  Map<String, String> maxValues;

  @JsonProperty("nullCount")
  Map<String, String> nullCount;

  public FileStats() {
    super();
  }

  public String getNumRecords() {
    return numRecords;
  }

  public Map<String, String> getMinValues() {
    return minValues;
  }

  public Map<String, String> getMaxValues() {
    return maxValues;
  }

  public Map<String, String> getNullCount() {
    return nullCount;
  }

  public FileStats(
      String numRecords,
      Map<String, String> minValues,
      Map<String, String> maxValues,
      Map<String, String> nullCount) {
    this.numRecords = numRecords;
    this.minValues = minValues;
    this.maxValues = maxValues;
    this.nullCount = nullCount;
  }
}
