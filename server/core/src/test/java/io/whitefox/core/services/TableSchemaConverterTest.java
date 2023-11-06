package io.whitefox.core.services;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.standalone.types.StructField;
import org.junit.jupiter.api.Test;

public class TableSchemaConverterTest {

  @Test
  public void testConvertDeltaSchemaToWhitefox() {
    var structType = new io.whitefox.core.types.StructType()
        .add("id", io.whitefox.core.types.IntegerType.INTEGER, false)
        .add("name", io.whitefox.core.types.StringType.STRING, true)
        .add("age", io.whitefox.core.types.IntegerType.INTEGER, true);

    io.delta.standalone.types.StructType deltaStructType =
        new io.delta.standalone.types.StructType(new StructField[] {
          new io.delta.standalone.types.StructField(
              "id",
              new io.delta.standalone.types.IntegerType(),
              false,
              io.delta.standalone.types.FieldMetadata.builder().build()),
          new io.delta.standalone.types.StructField(
              "name",
              new io.delta.standalone.types.StringType(),
              true,
              io.delta.standalone.types.FieldMetadata.builder().build()),
          new io.delta.standalone.types.StructField(
              "age",
              new io.delta.standalone.types.IntegerType(),
              true,
              io.delta.standalone.types.FieldMetadata.builder().build())
        });

    var convertedStructType =
        TableSchemaConverter.INSTANCE.convertDeltaSchemaToWhitefox(deltaStructType);

    assertTrue(structType.equivalent(convertedStructType));
  }

  @Test
  public void testConvertDeltaDataTypeToWhitefox() {
    assertEquals(
        io.whitefox.core.types.BooleanType.BOOLEAN,
        TableSchemaConverter.INSTANCE.convertDeltaDataTypeToWhitefox(
            new io.delta.standalone.types.BooleanType()));
    assertEquals(
        io.whitefox.core.types.ByteType.BYTE,
        TableSchemaConverter.INSTANCE.convertDeltaDataTypeToWhitefox(
            new io.delta.standalone.types.ByteType()));
    assertEquals(
        io.whitefox.core.types.ShortType.SHORT,
        TableSchemaConverter.INSTANCE.convertDeltaDataTypeToWhitefox(
            new io.delta.standalone.types.ShortType()));
    assertEquals(
        io.whitefox.core.types.IntegerType.INTEGER,
        TableSchemaConverter.INSTANCE.convertDeltaDataTypeToWhitefox(
            new io.delta.standalone.types.IntegerType()));
    assertEquals(
        io.whitefox.core.types.LongType.LONG,
        TableSchemaConverter.INSTANCE.convertDeltaDataTypeToWhitefox(
            new io.delta.standalone.types.LongType()));
    assertEquals(
        io.whitefox.core.types.FloatType.FLOAT,
        TableSchemaConverter.INSTANCE.convertDeltaDataTypeToWhitefox(
            new io.delta.standalone.types.FloatType()));
    assertEquals(
        io.whitefox.core.types.DoubleType.DOUBLE,
        TableSchemaConverter.INSTANCE.convertDeltaDataTypeToWhitefox(
            new io.delta.standalone.types.DoubleType()));
    assertEquals(
        io.whitefox.core.types.StringType.STRING,
        TableSchemaConverter.INSTANCE.convertDeltaDataTypeToWhitefox(
            new io.delta.standalone.types.StringType()));
    assertEquals(
        io.whitefox.core.types.BinaryType.BINARY,
        TableSchemaConverter.INSTANCE.convertDeltaDataTypeToWhitefox(
            new io.delta.standalone.types.BinaryType()));
    assertEquals(
        io.whitefox.core.types.DateType.DATE,
        TableSchemaConverter.INSTANCE.convertDeltaDataTypeToWhitefox(
            new io.delta.standalone.types.DateType()));
    assertEquals(
        io.whitefox.core.types.TimestampType.TIMESTAMP,
        TableSchemaConverter.INSTANCE.convertDeltaDataTypeToWhitefox(
            new io.delta.standalone.types.TimestampType()));
    assertEquals(
        new io.whitefox.core.types.DecimalType(10, 2),
        TableSchemaConverter.INSTANCE.convertDeltaDataTypeToWhitefox(
            new io.delta.standalone.types.DecimalType(10, 2)));
    assertEquals(
        new io.whitefox.core.types.ArrayType(io.whitefox.core.types.IntegerType.INTEGER, true),
        TableSchemaConverter.INSTANCE.convertDeltaDataTypeToWhitefox(
            new io.delta.standalone.types.ArrayType(
                new io.delta.standalone.types.IntegerType(), true)));
    assertEquals(
        new io.whitefox.core.types.MapType(
            io.whitefox.core.types.StringType.STRING,
            io.whitefox.core.types.IntegerType.INTEGER,
            true),
        TableSchemaConverter.INSTANCE.convertDeltaDataTypeToWhitefox(
            new io.delta.standalone.types.MapType(
                new io.delta.standalone.types.StringType(),
                new io.delta.standalone.types.IntegerType(),
                true)));
  }
}
