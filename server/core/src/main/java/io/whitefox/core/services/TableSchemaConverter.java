package io.whitefox.core.services;

import io.whitefox.core.types.*;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class TableSchemaConverter {

  public static final TableSchemaConverter INSTANCE = new TableSchemaConverter();

  public StructType convertDeltaSchemaToWhitefox(io.delta.standalone.types.StructType st) {
    var fields = st.getFields();
    var structType = new StructType();
    for (io.delta.standalone.types.StructField field : fields) {
      var name = field.getName();
      var dataType = field.getDataType();
      var nullable = field.isNullable();
      var metadata = field.getMetadata();
      structType = structType.add(new StructField(
          name,
          convertDeltaDataTypeToWhitefox(dataType),
          nullable,
          metadata.getEntries().entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> Objects.toString(e.getValue())))));
    }
    return structType;
  }

  public StructType convertIcebergSchemaToWhitefox(Types.StructType st) {
    var fields = st.fields();
    var structType = new StructType();
    for (Types.NestedField field : fields) {
      var name = field.name();
      var dataType = field.type();
      var nullable = field.isOptional();
      structType = structType.add(
          new StructField(name, convertIcebergDataTypeToWhitefox(dataType), nullable, Map.of()));
    }
    return structType;
  }

  public DataType convertDeltaDataTypeToWhitefox(io.delta.standalone.types.DataType st) {
    if (st instanceof io.delta.standalone.types.BooleanType) {
      return BooleanType.BOOLEAN;
    } else if (st instanceof io.delta.standalone.types.ByteType) {
      return ByteType.BYTE;
    } else if (st instanceof io.delta.standalone.types.ShortType) {
      return ShortType.SHORT;
    } else if (st instanceof io.delta.standalone.types.IntegerType) {
      return IntegerType.INTEGER;
    } else if (st instanceof io.delta.standalone.types.LongType) {
      return LongType.LONG;
    } else if (st instanceof io.delta.standalone.types.FloatType) {
      return FloatType.FLOAT;
    } else if (st instanceof io.delta.standalone.types.DoubleType) {
      return DoubleType.DOUBLE;
    } else if (st instanceof io.delta.standalone.types.StringType) {
      return StringType.STRING;
    } else if (st instanceof io.delta.standalone.types.BinaryType) {
      return BinaryType.BINARY;
    } else if (st instanceof io.delta.standalone.types.DateType) {
      return DateType.DATE;
    } else if (st instanceof io.delta.standalone.types.TimestampType) {
      return TimestampType.TIMESTAMP;
    } else if (st instanceof io.delta.standalone.types.DecimalType) {
      return new io.whitefox.core.types.DecimalType(
          ((io.delta.standalone.types.DecimalType) st).getPrecision(),
          ((io.delta.standalone.types.DecimalType) st).getScale());
    } else if (st instanceof io.delta.standalone.types.ArrayType) {
      return new ArrayType(
          convertDeltaDataTypeToWhitefox(
              ((io.delta.standalone.types.ArrayType) st).getElementType()),
          ((io.delta.standalone.types.ArrayType) st).containsNull());
    } else if (st instanceof io.delta.standalone.types.MapType) {
      return new io.whitefox.core.types.MapType(
          convertDeltaDataTypeToWhitefox(((io.delta.standalone.types.MapType) st).getKeyType()),
          convertDeltaDataTypeToWhitefox(((io.delta.standalone.types.MapType) st).getValueType()),
          ((io.delta.standalone.types.MapType) st).valueContainsNull());
    } else if (st instanceof io.delta.standalone.types.StructType) {
      return convertDeltaSchemaToWhitefox((io.delta.standalone.types.StructType) st);
    } else {
      throw new IllegalArgumentException("Unknown type: " + st);
    }
  }

  public DataType convertIcebergDataTypeToWhitefox(org.apache.iceberg.types.Type icebergType) {
    if (icebergType.isPrimitiveType()) {
      return convertIcebergPrimitiveTypeToWhitefox(icebergType.asPrimitiveType());
    } else if (icebergType.isListType()) {
      return new ArrayType(
          convertIcebergDataTypeToWhitefox(icebergType.asListType().elementType()),
          icebergType.asListType().isElementOptional());
    } else if (icebergType.isMapType()) {
      return new io.whitefox.core.types.MapType(
          convertIcebergDataTypeToWhitefox(icebergType.asMapType().keyType()),
          convertIcebergDataTypeToWhitefox(icebergType.asMapType().valueType()),
          icebergType.asMapType().isValueOptional());
    } else if (icebergType.isStructType()) {
      return convertIcebergSchemaToWhitefox(icebergType.asStructType());
    } else {
      throw new IllegalArgumentException("Unknown type: " + icebergType);
    }
  }

  private DataType convertIcebergPrimitiveTypeToWhitefox(Type.PrimitiveType primitiveType) {
    if (primitiveType instanceof Types.BooleanType) {
      return BooleanType.BOOLEAN;
    } else if (primitiveType instanceof Types.IntegerType) {
      return IntegerType.INTEGER;
    } else if (primitiveType instanceof Types.LongType) {
      return LongType.LONG;
    } else if (primitiveType instanceof Types.FloatType) {
      return FloatType.FLOAT;
    } else if (primitiveType instanceof Types.DoubleType) {
      return DoubleType.DOUBLE;
    } else if (primitiveType instanceof Types.StringType) {
      return StringType.STRING;
    } else if (primitiveType instanceof Types.BinaryType) {
      return BinaryType.BINARY;
    } else if (primitiveType instanceof Types.DateType) {
      return DateType.DATE;
    } else if (primitiveType instanceof Types.TimestampType) {
      return TimestampType.TIMESTAMP;
    } else if (primitiveType instanceof Types.DecimalType) {
      return new io.whitefox.core.types.DecimalType(
          ((Types.DecimalType) primitiveType).precision(),
          ((Types.DecimalType) primitiveType).scale());
    } else throw new RuntimeException(String.format("unknown primitive type: [%s]", primitiveType));
  }
}
