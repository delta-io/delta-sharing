package io.whitefox.api.deltasharing.model.v1.parquet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.whitefox.api.deltasharing.model.v1.Format;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ParquetMetadataSerializationTest {

  ObjectMapper om;

  ParquetMetadataSerializationTest() {
    om = new ObjectMapper();
    om.registerModule(new Jdk8Module());
  }

  @Test
  void serializationWithDefaultValuesTest() throws JsonProcessingException {
    var sut = ParquetMetadata.builder()
        .metadata(ParquetMetadata.Metadata.builder()
            .id("abc")
            .format(new Format())
            .partitionColumns(List.of())
            .schemaString("")
            .build())
        .build();

    Assertions.assertEquals(
        "{\"metaData\":{\"id\":\"abc\",\"format\":{\"provider\":\"parquet\"},\"schemaString\":\"\",\"partitionColumns\":[]}}",
        om.writer().writeValueAsString(sut));
  }

  @Test
  void serializationValuesTest() throws JsonProcessingException {
    var sut = ParquetMetadata.builder()
        .metadata(ParquetMetadata.Metadata.builder()
            .id("f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2")
            .format(new Format())
            .partitionColumns(List.of("date"))
            .schemaString(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}")
            .size(Optional.of(123456L))
            .numFiles(Optional.of(5L))
            .configuration(Optional.of(Map.of("enableChangeDataFeed", "true")))
            .build())
        .build();

    var expected =
        "{\"metaData\":{\"id\":\"f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2\",\"format\":{\"provider\":\"parquet\"},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"eventTime\\\",\\\"type\\\":\\\"timestamp\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"date\\\",\\\"type\\\":\\\"date\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionColumns\":[\"date\"],\"configuration\":{\"enableChangeDataFeed\":\"true\"},\"size\":123456,\"numFiles\":5}}";
    Assertions.assertEquals(expected, om.writer().writeValueAsString(sut));
  }

  @Test
  void deserializationTest() throws IOException {
    var input =
        "{\"metaData\":{\"partitionColumns\":[\"date\"],\"format\":{\"provider\":\"parquet\"},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"eventTime\\\",\\\"type\\\":\\\"timestamp\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"date\\\",\\\"type\\\":\\\"date\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"id\":\"f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2\",\"configuration\":{\"enableChangeDataFeed\":\"true\"},\"size\":123456,\"numFiles\":5}}";
    var expected = ParquetMetadata.builder()
        .metadata(ParquetMetadata.Metadata.builder()
            .id("f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2")
            .format(new Format())
            .partitionColumns(List.of("date"))
            .schemaString(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}")
            .size(Optional.of(123456L))
            .numFiles(Optional.of(5L))
            .configuration(Optional.of(Map.of("enableChangeDataFeed", "true")))
            .build())
        .build();
    Assertions.assertEquals(expected, om.reader().readValue(input, ParquetMetadata.class));
  }

  @Test
  void deserializationWithNullValuesTest() throws IOException {
    var expected = ParquetMetadata.builder()
        .metadata(ParquetMetadata.Metadata.builder()
            .id("abc")
            .format(new Format())
            .partitionColumns(List.of())
            .schemaString("")
            .build())
        .build();
    var input =
        "{\"metaData\":{\"id\":\"abc\",\"format\":{\"provider\":\"parquet\"},\"schemaString\":\"\",\"partitionColumns\":[]}}";
    Assertions.assertEquals(expected, om.reader().readValue(input, ParquetMetadata.class));
  }
}
