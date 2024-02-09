package io.whitefox.api.deltasharing.model.v1.delta;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DeltaInternalMetadataSerializationTest {
  String json =
      "{\"id\":\"f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2\",\"format\":{\"provider\":\"parquet\"},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"eventTime\\\",\\\"type\\\":\\\"timestamp\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"date\\\",\\\"type\\\":\\\"date\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionColumns\":[\"date\"],\"configuration\":{\"enableChangeDataFeed\":\"true\"}}";
  DeltaInternalMetadata object = DeltaInternalMetadata.builder()
      .partitionColumns(List.of("date"))
      .format(new DeltaInternalFormat(Optional.empty()))
      .schemaString(
          "{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}")
      .id("f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2")
      .configuration(Map.of("enableChangeDataFeed", "true"))
      .build();
  ObjectMapper om;

  DeltaInternalMetadataSerializationTest() {
    om = new ObjectMapper();
    om.registerModule(new Jdk8Module());
  }

  @Test
  void deserialize() throws IOException {
    Assertions.assertEquals(object, om.reader().readValue(json, DeltaInternalMetadata.class));
  }

  @Test
  void serialize() throws JsonProcessingException {
    Assertions.assertEquals(json, om.writer().writeValueAsString(object));
  }
}
