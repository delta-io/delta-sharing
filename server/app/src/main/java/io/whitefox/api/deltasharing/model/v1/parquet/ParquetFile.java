package io.whitefox.api.deltasharing.model.v1.parquet;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@SuperBuilder
@Jacksonized
public class ParquetFile {

  @JsonProperty
  @NonNull ParquetFile.File file;

  @Value
  @Builder
  @Jacksonized
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  public static class File {

    /**
     * A https url that a client can use to read the file directly.
     * The same file in different responses may have different urls.
     */
    @JsonProperty
    @NonNull String url;

    /**
     * A unique string for the file in a table.
     * The same file is guaranteed to have the same id across multiple requests.
     * A client may cache the file content and use this id as a key to decide whether to use the cached file content.
     */
    @JsonProperty
    @NonNull String id;

    /**
     * A map from partition column to value for this file.
     * When the table doesn’t have partition columns, this will be an empty map.
     * See Partition Value Serialization for how to parse the partition values.
     */
    @JsonProperty
    @NonNull Map<String, String> partitionValues;

    /**
     * The size of this file in bytes.
     */
    @JsonProperty
    long size;

    /**
     * Contains statistics (e.g., count, min/max values for columns) about the data in this file.
     * This field may be missing. A file may or may not have stats.
     * This is a serialized JSON string which can be deserialized to a Statistics Struct.
     * A client can decide whether to use stats or drop it.
     */
    @JsonProperty
    @Builder.Default
    Optional<String> stats = Optional.empty();

    /**
     * The table version of the file, returned when querying a table data with a version or timestamp parameter.
     */
    @JsonProperty
    @Builder.Default
    Optional<Long> version = Optional.empty();

    /**
     * The unix timestamp corresponding to the table version of the file, in milliseconds,
     * returned when querying a table data with a version or timestamp parameter.
     */
    @JsonProperty
    @Builder.Default
    Optional<Long> timestamp = Optional.empty();

    /**
     * The unix timestamp corresponding to the expiration of the url, in milliseconds,
     * returned when the server supports the feature.
     */
    @JsonProperty
    @Builder.Default
    Optional<Long> expirationTimestamp = Optional.empty();
  }
}
