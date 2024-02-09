package io.whitefox.api.deltasharing.model.v1;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class Format {
  private static final String PARQUET = "parquet";

  @JsonProperty
  public String provider() {
    return PARQUET;
  }

  public Format() {
    this(PARQUET);
  }

  @JsonCreator
  private Format(@JsonProperty("provider") String provider) {
    if (!"parquet".equalsIgnoreCase(provider)) {
      throw new IllegalArgumentException("Provider must be " + PARQUET);
    }
  }
}
