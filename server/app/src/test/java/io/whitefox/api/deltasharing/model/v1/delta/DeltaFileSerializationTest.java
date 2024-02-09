package io.whitefox.api.deltasharing.model.v1.delta;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.io.IOException;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DeltaFileSerializationTest {
  String singleActionJson;
  String json;

  DeltaFile object;
  ObjectMapper om;

  public DeltaFileSerializationTest() throws JsonProcessingException {
    om = new ObjectMapper();
    om.registerModule(new Jdk8Module());
    singleActionJson =
        "{\"add\":{\"dataChange\":false,\"modificationTime\":0,\"partitionValues\":{\"date\":\"2021-04-28\"},\"path\":\"https://s3-bucket-name.s3.us-west-2.amazonaws.com/delta-exchange-test/table2/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?...\",\"pathAsUri\":\"https://s3-bucket-name.s3.us-west-2.amazonaws.com/delta-exchange-test/table2/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?...\",\"size\":0,\"stats\":\"{\\\"numRecords\\\":1,\\\"minValues\\\":{\\\"eventTime\\\":\\\"2021-04-28T23:33:48.719Z\\\"},\\\"maxValues\\\":{\\\"eventTime\\\":\\\"2021-04-28T23:33:48.719Z\\\"},\\\"nullCount\\\":{\\\"eventTime\\\":0}}\"}}";
    json =
        "{\"file\":{\"id\":\"591723a8-6a27-4240-a90e-57426f4736d2\",\"expirationTimestamp\":1652140800000,\"deltaSingleAction\":"
            + singleActionJson + "}}";
    object = DeltaFile.builder()
        .file(DeltaFile.File.builder()
            .id("591723a8-6a27-4240-a90e-57426f4736d2")
            .expirationTimestamp(Optional.of(1652140800000L))
            .deltaSingleAction(om.readTree(singleActionJson))
            .build())
        .build();
  }

  @Test
  void serialize() throws IOException {
    Assertions.assertEquals(json, om.writer().writeValueAsString(object));
  }

  @Test
  void deserialize() throws IOException {
    Assertions.assertEquals(object, om.reader().readValue(json, DeltaFile.class));
  }
}
