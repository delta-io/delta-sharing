package io.whitefox.core;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.whitefox.core.types.DataType;
import io.whitefox.core.types.predicates.DataTypeDeserializer;

public class DeltaObjectMapper {

  private static final ObjectMapper objectMapper = newInstance();

  private static ObjectMapper newInstance() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    var customSerializersModule = new SimpleModule();
    customSerializersModule.addDeserializer(DataType.class, new DataTypeDeserializer());
    mapper.registerModule(customSerializersModule);
    return mapper;
  }

  public static ObjectMapper getInstance() {
    return objectMapper;
  }
}
