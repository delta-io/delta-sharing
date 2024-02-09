package io.whitefox.api.deltasharing.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.whitefox.api.deltasharing.model.v1.TableQueryResponse;
import io.whitefox.api.deltasharing.model.v1.parquet.ParquetFile;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class TableQueryResponseSerializer implements Serializer<TableQueryResponse> {
  private final ObjectWriter objectWriter;
  private static final String LINE_FEED = "\n";

  @Inject
  public TableQueryResponseSerializer(ObjectMapper objectMapper) {
    this.objectWriter = objectMapper.writer();
  }

  @Override
  public String serialize(TableQueryResponse data) {
    StringBuilder stringBuilder = new StringBuilder();
    try {
      stringBuilder.append(objectWriter.writeValueAsString(data.protocol()));
      stringBuilder.append(LINE_FEED);
      stringBuilder.append(objectWriter.writeValueAsString(data.metadata()));
      for (ParquetFile line : data.files()) {
        stringBuilder.append(LINE_FEED);
        stringBuilder.append(objectWriter.writeValueAsString(line));
      }
      return stringBuilder.toString();
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
