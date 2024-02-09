package io.whitefox.api.deltasharing;

import io.micrometer.common.util.StringUtils;
import io.whitefox.api.deltasharing.errors.InvalidDeltaSharingCapabilities;
import io.whitefox.api.server.DeltaHeaders;
import io.whitefox.core.services.capabilities.ClientCapabilities;
import io.whitefox.core.services.capabilities.ReaderFeatures;
import io.whitefox.core.services.capabilities.ResponseFormat;
import io.whitefox.core.services.exceptions.UnknownResponseFormat;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ApplicationScoped
public class ClientCapabilitiesMapper implements DeltaHeaders {

  /**
   * @param header the string representation of the capabilities of the delta-sharing client
   * @return the clean and business oriented version of it
   */
  public ClientCapabilities parseDeltaSharingCapabilities(String header) {

    if (header == null) {
      return ClientCapabilities.parquet();
    } else {
      Map<String, Set<String>> rawValues = Arrays.stream(header.split(";", -1))
          .flatMap(entry -> {
            if (StringUtils.isBlank(entry)) {
              return Stream.empty();
            }
            var keyAndValues = entry.split("=", -1);
            if (keyAndValues.length != 2) {
              throw new InvalidDeltaSharingCapabilities(String.format(
                  "Each %s must be in the format key=value", DELTA_SHARE_CAPABILITIES_HEADER));
            }
            var key = keyAndValues[0];
            var values = Arrays.stream(keyAndValues[1].split(",", -1))
                .collect(Collectors.toUnmodifiableSet());
            return Stream.of(Map.entry(key, values));
          })
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      Set<String> responseFormats = rawValues.get(DELTA_SHARING_RESPONSE_FORMAT);
      String theResponseFormat = pickResponseFormat(responseFormats);
      if (ResponseFormat.parquet.stringRepresentation().equalsIgnoreCase(theResponseFormat)) {
        return ClientCapabilities.parquet();
      } else if (ResponseFormat.delta.stringRepresentation().equalsIgnoreCase(theResponseFormat)) {
        var unparsed =
            Optional.ofNullable(rawValues.get(DELTA_SHARING_READER_FEATURES)).orElse(Set.of());
        return ClientCapabilities.delta(unparsed.stream()
            .map(ReaderFeatures::fromString)
            .flatMap(Optional::stream)
            .collect(Collectors.toSet()));
      } else {
        throw new UnknownResponseFormat(
            String.format("Unknown response format %s", theResponseFormat));
      }
    }
  }

  private String pickResponseFormat(Set<String> responseFormats) {
    if (responseFormats == null || responseFormats.isEmpty()) {
      return ResponseFormat.parquet.stringRepresentation();
    } else {
      // Quoting the protocol:
      // > If there's a list of responseFormat specified, such as responseFormat=delta,parquet.
      // > The server may choose to respond in parquet format if the table does not have any
      // advanced features.
      // > The server must respond in delta format if the table has advanced features which are not
      // compatible
      // > with the parquet format.
      // so here we choose to return delta (if present) so that the service can choose to downgrade
      // it
      // for compatibility reasons
      if (responseFormats.contains(ResponseFormat.delta.stringRepresentation())) {
        return ResponseFormat.delta.stringRepresentation();
      } else {
        return responseFormats.iterator().next();
      }
    }
  }
}
