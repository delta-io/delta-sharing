package io.whitefox.api.deltasharing.model.v1.delta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Optional;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
@JsonInclude(JsonInclude.Include.NON_ABSENT)
public class DeltaInternalFormat {
  private static final String PARQUET = "parquet";

  private final Optional<Map<String, String>> options;

  @JsonProperty
  public Optional<Map<String, String>> options() {
    return options;
  }

  @JsonProperty
  public String provider() {
    return PARQUET;
  }

  @JsonCreator
  private DeltaInternalFormat(
      @JsonProperty("provider") String provider,
      @JsonProperty("options") Map<String, String> options) {

    if (!PARQUET.equalsIgnoreCase(provider)) {
      throw new IllegalArgumentException("Provider must be " + PARQUET);
    }
    this.options = Optional.ofNullable(options).map(Map::copyOf);
  }

  public DeltaInternalFormat(Optional<Map<String, String>> options) {
    this(PARQUET, options.orElse(null));
  }
}
