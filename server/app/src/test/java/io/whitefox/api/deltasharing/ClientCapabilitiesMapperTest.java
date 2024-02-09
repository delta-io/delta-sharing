package io.whitefox.api.deltasharing;

import io.whitefox.api.server.DeltaHeaders;
import io.whitefox.core.services.capabilities.CapabilitiesConstants;
import io.whitefox.core.services.capabilities.ReaderFeatures;
import io.whitefox.core.services.capabilities.ResponseFormat;
import io.whitefox.core.services.exceptions.UnknownResponseFormat;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ClientCapabilitiesMapperTest implements DeltaHeaders {

  ClientCapabilitiesMapper mapper = new ClientCapabilitiesMapper();
  String responseFormatDelta = "responseformat=delta";

  @Test
  void parseSimpleResponseFormatDelta() {
    Assertions.assertEquals(
        ResponseFormat.delta,
        mapper.parseDeltaSharingCapabilities(responseFormatDelta).responseFormat());
  }

  @Test
  void parseSimpleResponseFormatParquet() {
    Assertions.assertEquals(
        ResponseFormat.parquet,
        mapper.parseDeltaSharingCapabilities("responseformat=PaRquEt").responseFormat());
  }

  @Test
  void failToParseUnknownResponseFormatAndFail() {
    Assertions.assertThrows(
        UnknownResponseFormat.class,
        () -> mapper.parseDeltaSharingCapabilities("responseformat=iceberg").responseFormat());
  }

  @Test
  void failToParseUnknownResponseFormatAndReturnOthers() {
    Assertions.assertEquals(
        ResponseFormat.delta,
        mapper
            .parseDeltaSharingCapabilities("responseformat=iceberg,parquet,delta")
            .responseFormat());
  }

  @Test
  void noCapabilitiesEqualsDefault() {
    Assertions.assertEquals(
        ResponseFormat.parquet,
        mapper.parseDeltaSharingCapabilities((String) null).responseFormat());
    Assertions.assertEquals(
        Set.of(), mapper.parseDeltaSharingCapabilities((String) null).readerFeatures());
    Assertions.assertEquals(
        ResponseFormat.parquet, mapper.parseDeltaSharingCapabilities("").responseFormat());
    Assertions.assertEquals(Set.of(), mapper.parseDeltaSharingCapabilities("").readerFeatures());
  }

  @Test
  void parseSimpleReaderFeature() {
    Assertions.assertEquals(
        Set.of(ReaderFeatures.DELETION_VECTORS),
        mapper
            .parseDeltaSharingCapabilities(String.format(
                responseFormatDelta + ";%s=%s",
                DELTA_SHARING_READER_FEATURES,
                CapabilitiesConstants.DELTA_SHARING_READER_FEATURE_DELETION_VECTOR))
            .readerFeatures());
  }

  @Test
  void failToParseUnknownReaderFeatureAndReturnNothing() {
    Assertions.assertEquals(
        Set.of(),
        mapper
            .parseDeltaSharingCapabilities(
                String.format("%s=%s", DELTA_SHARING_READER_FEATURES, "unknown"))
            .readerFeatures());
  }

  @Test
  void failToParseUnknownReaderFeatureAndReturnOthers() {
    Assertions.assertEquals(
        Set.of(ReaderFeatures.COLUMN_MAPPING, ReaderFeatures.DOMAIN_METADATA),
        mapper
            .parseDeltaSharingCapabilities(String.format(
                responseFormatDelta + ";%s=%s,%s,%s",
                DELTA_SHARING_READER_FEATURES,
                "unknown",
                CapabilitiesConstants.DELTA_SHARING_READER_FEATURE_COLUMN_MAPPING,
                CapabilitiesConstants.DELTA_SHARING_READER_FEATURE_DOMAIN_METADATA))
            .readerFeatures());
  }

  @Test
  void kitchenSink() {
    var readerFeatures = String.format(
        "%s=%s,%s,%s",
        DELTA_SHARING_READER_FEATURES,
        "unknown",
        CapabilitiesConstants.DELTA_SHARING_READER_FEATURE_COLUMN_MAPPING,
        CapabilitiesConstants.DELTA_SHARING_READER_FEATURE_DOMAIN_METADATA);
    var responseFormat = "responseformat=iceberg,parquet,delta";
    var capabilities = mapper.parseDeltaSharingCapabilities(
        String.format("%s;%s", readerFeatures, responseFormat));
    Assertions.assertEquals(ResponseFormat.delta, capabilities.responseFormat());
    Assertions.assertEquals(
        Set.of(ReaderFeatures.COLUMN_MAPPING, ReaderFeatures.DOMAIN_METADATA),
        capabilities.readerFeatures());
  }
}
