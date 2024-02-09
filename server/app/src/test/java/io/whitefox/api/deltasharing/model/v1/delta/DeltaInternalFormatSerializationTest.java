package io.whitefox.api.deltasharing.model.v1.delta;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DeltaInternalFormatSerializationTest {
  String json = "{\"provider\":\"parquet\",\"options\":{\"key\":\"value\"}}";
  DeltaInternalFormat object = new DeltaInternalFormat(Optional.of(Map.of("key", "value")));

  ObjectMapper om;

  DeltaInternalFormatSerializationTest() {
    om = new ObjectMapper();
    om.registerModule(new Jdk8Module());
  }

  @Test
  void deserialize() throws IOException {
    Assertions.assertEquals(object, om.reader().readValue(json, DeltaInternalFormat.class));
  }

  @Test
  void deserializeWithoutOptions() throws IOException {
    var json = "{\"provider\":\"parquet\"}";
    var object = new DeltaInternalFormat(Optional.empty());
    Assertions.assertEquals(object, om.reader().readValue(json, DeltaInternalFormat.class));
  }

  @Test
  void serialize() throws JsonProcessingException {
    Assertions.assertEquals(json, om.writer().writeValueAsString(object));
  }
}
