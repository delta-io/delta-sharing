package io.whitefox.api.deltasharing.model.v1.delta;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@SuperBuilder
@Jacksonized
@Value
public class DeltaProtocol {

  @JsonProperty
  Protocol protocol;

  @SuperBuilder
  @Jacksonized
  @Value
  private static class Protocol {
    @JsonProperty
    InternalDeltaProtocol deltaProtocol;
  }

  @SuperBuilder
  @Jacksonized
  @Value
  private static class InternalDeltaProtocol {
    @JsonProperty
    int minReaderVersion;

    @JsonProperty
    int minWriterVersion;
  }

  public static DeltaProtocol of(int minReaderVersion, int maxReaderVersion) {
    return DeltaProtocol.builder()
        .protocol(Protocol.builder()
            .deltaProtocol(InternalDeltaProtocol.builder()
                .minReaderVersion(minReaderVersion)
                .minWriterVersion(maxReaderVersion)
                .build())
            .build())
        .build();
  }
}
