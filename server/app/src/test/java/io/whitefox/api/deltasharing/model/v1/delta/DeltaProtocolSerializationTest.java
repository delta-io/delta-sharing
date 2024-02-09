package io.whitefox.api.deltasharing.model.v1.delta;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DeltaProtocolSerializationTest {
  ObjectMapper om = new ObjectMapper();

  @Test
  void serializationTest() throws JsonProcessingException {
    var expected =
        "{\"protocol\":{\"deltaProtocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7}}}";
    Assertions.assertEquals(expected, om.writer().writeValueAsString(DeltaProtocol.of(3, 7)));
  }

  @Test
  void deserializationTest() throws IOException {
    var input =
        "{\"protocol\":{\"deltaProtocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7}}}";
    var result = om.reader().readValue(input, DeltaProtocol.class);
    Assertions.assertEquals(DeltaProtocol.of(3, 7), result);
  }
}
