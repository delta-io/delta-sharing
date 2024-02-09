package io.whitefox.api.deltasharing.model.v1.delta;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@SuperBuilder
@Jacksonized
@Value
@JsonInclude(JsonInclude.Include.NON_ABSENT)
public class DeltaInternalMetadata {

  /**
   * Unique identifier for this table.
   */
  @JsonProperty
  @NonNull String id;

  /**
   * User-provided identifier for this table.
   */
  @JsonProperty
  @Builder.Default
  Optional<String> name = Optional.empty();

  /**
   * User-provided description for this table.
   */
  @JsonProperty
  @Builder.Default
  Optional<String> description = Optional.empty();

  /**
   * Specification of the encoding for the files stored in the table.
   */
  @JsonProperty
  @NonNull DeltaInternalFormat format;

  /**
   * Schema of the table.
   */
  @JsonProperty
  @NonNull String schemaString;

  /**
   * An array containing the names of columns by which the data should be partitioned.
   */
  @JsonProperty
  @NonNull List<String> partitionColumns;

  /**
   * The time when this metadata action is created, in milliseconds since the Unix epoch.
   */
  @JsonProperty
  @Builder.Default
  Optional<Long> createdTime = Optional.empty();

  /**
   * A map containing configuration options for the metadata action.
   */
  @JsonProperty
  @NonNull Map<String, String> configuration;
}
