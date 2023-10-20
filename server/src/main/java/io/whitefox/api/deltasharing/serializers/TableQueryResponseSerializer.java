package io.whitefox.api.deltasharing.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.whitefox.api.deltasharing.model.v1.generated.FileObject;
import io.whitefox.api.deltasharing.model.v1.generated.TableQueryResponseObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class TableQueryResponseSerializer implements Serializer<TableQueryResponseObject> {
  private final ObjectWriter objectWriter;
  private static final String LINE_FEED = "\n";

  @Inject
  public TableQueryResponseSerializer(ObjectMapper objectMapper) {
    this.objectWriter = objectMapper.writer();
  }

  @Override
  public String serialize(TableQueryResponseObject data) {
    StringBuilder stringBuilder = new StringBuilder();
    try {
      stringBuilder.append(objectWriter.writeValueAsString(data.getProtocol()));
      stringBuilder.append(LINE_FEED);
      stringBuilder.append(objectWriter.writeValueAsString(data.getMetadata()));
      for (FileObject line : data.getFiles()) {
        stringBuilder.append(LINE_FEED);
        stringBuilder.append(objectWriter.writeValueAsString(line));
      }
      return stringBuilder.toString();
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
