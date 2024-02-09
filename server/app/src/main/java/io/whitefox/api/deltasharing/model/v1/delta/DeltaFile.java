package io.whitefox.api.deltasharing.model.v1.delta;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Optional;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@SuperBuilder
@Jacksonized
public class DeltaFile {

  @JsonProperty
  @NonNull File file;

  @Value
  @SuperBuilder
  @Jacksonized
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  public static class File {

    /**
     * A unique string for the file in a table.
     * The same file is guaranteed to have the same id across multiple requests.
     * A client may cache the file content and use this id as a key to decide whether to use the cached file content.
     */
    @JsonProperty
    @NonNull String id;

    /**
     * A unique string for the deletion vector file in a table.
     * The same deletion vector file is guaranteed to have the same id across multiple requests.
     * A client may cache the file content and use this id as a key to decide whether to use the cached file content.
     */
    @JsonProperty
    @Builder.Default
    Optional<String> deletionVectorFileId = Optional.empty();

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

    /**
     * Need to be parsed by a delta library as a delta single action, the path field is replaced by pr-signed url.
     */
    @JsonProperty
    @NonNull JsonNode deltaSingleAction;
  }
}
