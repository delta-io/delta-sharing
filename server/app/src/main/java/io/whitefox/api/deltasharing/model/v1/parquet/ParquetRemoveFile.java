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
public class ParquetRemoveFile {
  @JsonProperty
  @NonNull Remove remove;

  @Value
  @Builder
  @Jacksonized
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  public static class Remove {
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
     * The timestamp of the file in milliseconds from epoch.
     */
    @JsonProperty
    long timestamp;

    /**
     * The table version of this file.
     */
    @JsonProperty
    int version;

    /**
     * The unix timestamp corresponding to the expiration of the url, in milliseconds,
     * returned when the server supports the feature.
     */
    @JsonProperty
    @Builder.Default
    Optional<Long> expirationTimestamp = Optional.empty();
  }
}
