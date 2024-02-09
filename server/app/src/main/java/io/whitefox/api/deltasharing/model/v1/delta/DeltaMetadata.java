package io.whitefox.api.deltasharing.model.v1.delta;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Optional;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@SuperBuilder
@Jacksonized
class DeltaMetadata {
  @JsonProperty("metaData")
  @NonNull Metadata metadata;

  @Value
  @SuperBuilder
  @Jacksonized
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  public static class Metadata {

    /**
     * Need to be parsed by a delta library as delta metadata.
     */
    @JsonProperty
    @NonNull DeltaInternalMetadata deltaMetadata;

    /**
     * The table version the metadata corresponds to, returned when querying table data with a version or timestamp parameter,
     * or cdf query with includeHistoricalMetadata set to true.
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
