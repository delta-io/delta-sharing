package io.whitefox.api.deltasharing.model.v1.parquet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ParquetProtocolSerializationTest {
  ObjectMapper om = new ObjectMapper();

  @Test
  void serializeProtocol() throws JsonProcessingException {
    Assertions.assertEquals(
        "{\"protocol\":{\"minReaderVersion\":1}}",
        om.writer().writeValueAsString(ParquetProtocol.ofMinReaderVersion(1)));
  }

  @Test
  void deserializeProtocol() throws IOException {
    var result =
        om.reader().readValue("{\"protocol\":{\"minReaderVersion\":1}}", ParquetProtocol.class);
    Assertions.assertEquals(ParquetProtocol.ofMinReaderVersion(1), result);
  }
}
