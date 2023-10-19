package io.whitefox.core.types;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class MapTypeTest {

  @Test
  public void testConstructor() {
    DataType keyType = StringType.STRING;
    DataType valueType = IntegerType.INTEGER;
    boolean valueContainsNull = true;

    MapType mapType = new MapType(keyType, valueType, valueContainsNull);

    assertEquals(keyType, mapType.getKeyType());
    assertEquals(valueType, mapType.getValueType());
    assertEquals(valueContainsNull, mapType.isValueContainsNull());
  }

  @Test
  public void testGetKeyType() {
    DataType keyType = StringType.STRING;
    DataType valueType = IntegerType.INTEGER;
    boolean valueContainsNull = true;

    MapType mapType = new MapType(keyType, valueType, valueContainsNull);

    assertEquals(keyType, mapType.getKeyType());
  }

  @Test
  public void testGetValueType() {
    DataType keyType = StringType.STRING;
    DataType valueType = IntegerType.INTEGER;
    boolean valueContainsNull = true;

    MapType mapType = new MapType(keyType, valueType, valueContainsNull);

    assertEquals(valueType, mapType.getValueType());
  }

  @Test
  public void testIsValueContainsNull() {
    DataType keyType = StringType.STRING;
    DataType valueType = IntegerType.INTEGER;
    boolean valueContainsNull = true;

    MapType mapType = new MapType(keyType, valueType, valueContainsNull);

    assertTrue(mapType.isValueContainsNull());
  }

  @Test
  public void testEquivalent() {
    DataType keyType1 = StringType.STRING;
    DataType valueType1 = IntegerType.INTEGER;
    boolean valueContainsNull1 = true;

    DataType keyType2 = StringType.STRING;
    DataType valueType2 = IntegerType.INTEGER;
    boolean valueContainsNull2 = true;

    DataType keyType3 = IntegerType.INTEGER;
    DataType valueType3 = StringType.STRING;
    boolean valueContainsNull3 = false;

    MapType mapType1 = new MapType(keyType1, valueType1, valueContainsNull1);
    MapType mapType2 = new MapType(keyType2, valueType2, valueContainsNull2);
    MapType mapType3 = new MapType(keyType3, valueType3, valueContainsNull3);

    assertTrue(mapType1.equivalent(mapType2));
    assertFalse(mapType1.equivalent(mapType3));
  }

  @Test
  public void testToJson() {
    DataType keyType = StringType.STRING;
    DataType valueType = IntegerType.INTEGER;
    boolean valueContainsNull = true;

    MapType mapType = new MapType(keyType, valueType, valueContainsNull);

    assertEquals(
        "{\"type\": \"map\",\"keyType\": \"string\",\"valueType\": \"integer\",\"valueContainsNull\": true}",
        mapType.toJson());
  }
}
