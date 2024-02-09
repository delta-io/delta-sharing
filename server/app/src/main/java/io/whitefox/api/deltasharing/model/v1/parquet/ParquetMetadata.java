package io.whitefox.api.deltasharing.model.v1.parquet;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.whitefox.api.deltasharing.model.v1.Format;
import java.util.List;
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
public class ParquetMetadata {
  @JsonProperty(value = "metaData")
  @NonNull Metadata metadata;

  @Value
  @SuperBuilder
  @Jacksonized
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  public static class Metadata {

    /**
     * Unique identifier for this table
     */
    @JsonProperty
    @NonNull String id;

    /**
     * User-provided identifier for this table
     */
    @JsonProperty
    @Builder.Default
    Optional<String> name = Optional.empty();

    /**
     * User-provided description for this table
     */
    @JsonProperty
    @Builder.Default
    Optional<String> description = Optional.empty();

    /**
     * Specification of the encoding for the files stored in the table.
     */
    @JsonProperty
    @NonNull Format format;

    /**
     * Schema of the table. This is a serialized JSON string which can be deserialized to a Schema Object.
     */
    @JsonProperty
    @NonNull String schemaString;

    /**
     * An array containing the names of columns by which the data should be partitioned. When a table doesn’t have partition columns, this will be an empty array.
     */
    @JsonProperty
    @NonNull List<String> partitionColumns;

    /**
     * A map containing configuration options for the table
     */
    @JsonProperty
    @Builder.Default
    Optional<Map<String, String>> configuration = Optional.empty();

    /**
     * The table version the metadata corresponds to, returned when querying table data with a version or timestamp parameter, or cdf query with includeHistoricalMetadata set to true.
     */
    @JsonProperty
    @Builder.Default
    Optional<Long> version = Optional.empty();

    /**
     * The size of the table in bytes, will be returned if available in the delta log.
     */
    @JsonProperty
    @Builder.Default
    Optional<Long> size = Optional.empty();

    /**
     * The number of files in the table, will be returned if available in the delta log.
     */
    @JsonProperty
    @Builder.Default
    Optional<Long> numFiles = Optional.empty();
  }
}
