package io.whitefox.api.deltasharing.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.whitefox.api.deltasharing.server.restdto.TableResponseMetadata;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TableMetadataSerializer implements Serializer<TableResponseMetadata> {
  private final ObjectWriter objectWriter;
  private static final String LINE_FEED = "\n";

  public TableMetadataSerializer() {
    this.objectWriter = new ObjectMapper().writer();
  }

  @Override
  public String serialize(TableResponseMetadata data) {
    StringBuilder stringBuilder = new StringBuilder();
    try {
      stringBuilder.append(objectWriter.writeValueAsString(data.getProtocol()));
      stringBuilder.append(LINE_FEED);
      stringBuilder.append(objectWriter.writeValueAsString(data.getMetadata()));
      return stringBuilder.toString();
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
