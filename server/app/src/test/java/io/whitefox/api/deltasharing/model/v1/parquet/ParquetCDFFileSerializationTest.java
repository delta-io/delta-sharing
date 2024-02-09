package io.whitefox.api.deltasharing.model.v1.parquet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ParquetCDFFileSerializationTest {
  String json =
      "{\"cdf\":{\"url\":\"https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table_cdf/_change_data/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010655Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=dd5d3ba1a179dc7e239d257feed046dccc95000d1aa0479ea6ff36d10d90ec94\",\"id\":\"591723a8-6a27-4240-a90e-57426f4736d2\",\"partitionValues\":{\"date\":\"2021-04-28\"},\"size\":573,\"timestamp\":1652140800000,\"version\":1,\"expirationTimestamp\":1652144400000}}";

  ParquetCDFFile object = ParquetCDFFile.builder()
      .cdf(ParquetCDFFile.CDF
          .builder()
          .url(
              "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table_cdf/_change_data/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010655Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=dd5d3ba1a179dc7e239d257feed046dccc95000d1aa0479ea6ff36d10d90ec94")
          .id("591723a8-6a27-4240-a90e-57426f4736d2")
          .size(573)
          .partitionValues(Map.of("date", "2021-04-28"))
          .timestamp(1652140800000L)
          .version(1)
          .expirationTimestamp(Optional.of(1652144400000L))
          .build())
      .build();
  ObjectMapper om;

  ParquetCDFFileSerializationTest() {
    om = new ObjectMapper();
    om.registerModule(new Jdk8Module());
  }

  @Test
  void deserialize() throws IOException {
    Assertions.assertEquals(object, om.reader().readValue(json, ParquetCDFFile.class));
  }

  @Test
  void serialize() throws JsonProcessingException {
    Assertions.assertEquals(json, om.writer().writeValueAsString(object));
  }
}
