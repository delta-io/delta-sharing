package io.whitefox.api.deltasharing.model.v1.parquet;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@SuperBuilder
@Jacksonized
@Value
public class ParquetProtocol {
  @JsonProperty
  Protocol protocol;

  @SuperBuilder
  @Jacksonized
  @Value
  private static class Protocol {
    @JsonProperty
    int minReaderVersion;
  }

  public static ParquetProtocol ofMinReaderVersion(int minReaderVersion) {
    return ParquetProtocol.builder()
        .protocol(Protocol.builder().minReaderVersion(minReaderVersion).build())
        .build();
  }
}
