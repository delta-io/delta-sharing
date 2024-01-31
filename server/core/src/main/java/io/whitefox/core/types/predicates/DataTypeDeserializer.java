package io.whitefox.core.types.predicates;

import static io.whitefox.core.types.DateType.DATE;
import static io.whitefox.core.types.DoubleType.DOUBLE;
import static io.whitefox.core.types.FloatType.FLOAT;
import static io.whitefox.core.types.IntegerType.INTEGER;
import static io.whitefox.core.types.LongType.LONG;
import static io.whitefox.core.types.StringType.STRING;
import static io.whitefox.core.types.TimestampType.TIMESTAMP;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.whitefox.core.types.*;
import java.io.IOException;

public class DataTypeDeserializer extends StdDeserializer<DataType> {

  // needed for jackson
  public DataTypeDeserializer() {
    this(null);
  }

  public DataTypeDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public DataType deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException {
    JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    String valueType = node.asText();
    DataType primitive = BasePrimitiveType.createPrimitive(valueType);
    if (DATE.equals(primitive)) {
      return DATE;
    } else if (INTEGER.equals(primitive)) {
      return INTEGER;
    } else if (DOUBLE.equals(primitive)) {
      return DOUBLE;
    } else if (FLOAT.equals(primitive)) {
      return FLOAT;
    } else if (STRING.equals(primitive)) {
      return STRING;
    } else if (TIMESTAMP.equals(primitive)) {
      return TIMESTAMP;
    } else if (LONG.equals(primitive)) {
      return LONG;
    }
    throw new JsonParseException("Unknown type passed inside a json predicate: " + valueType);
  }
}
