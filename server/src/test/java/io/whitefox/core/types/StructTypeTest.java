package io.whitefox.core.types;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class StructTypeTest {

  @Test
  public void testAddField() {
    StructType structType = new StructType();
    StructType newStructType = structType.add("name", StringType.STRING, true);

    assertEquals(0, structType.length());
    assertEquals(1, newStructType.length());
    assertEquals("name", newStructType.at(0).getName());
    assertEquals(StringType.STRING, newStructType.at(0).getDataType());
    assertTrue(newStructType.at(0).isNullable());
  }

  @Test
  public void testEquals() {
    StructType structType1 = new StructType();
    StructType structType2 = new StructType();

    assertEquals(structType1, structType2);

    StructType newStructType = structType1.add("name", StringType.STRING, true);
    assertNotEquals(structType1, newStructType);
  }

  @Test
  public void testFields() {
    StructType structType = new StructType();
    StructType newStructType = structType.add("name", StringType.STRING, true);

    assertEquals(0, structType.fields().size());
    assertEquals(1, newStructType.fields().size());
    assertEquals("name", newStructType.fields().get(0).getName());
    assertEquals(StringType.STRING, newStructType.fields().get(0).getDataType());
    assertTrue(newStructType.fields().get(0).isNullable());
  }

  @Test
  public void testFieldNames() {
    StructType structType = new StructType();
    StructType newStructType = structType.add("name", StringType.STRING, true);

    assertEquals(0, structType.fieldNames().size());
    assertEquals(1, newStructType.fieldNames().size());
    assertEquals("name", newStructType.fieldNames().get(0));
  }

  @Test
  public void testLength() {
    StructType structType = new StructType();
    StructType newStructType = structType.add("name", StringType.STRING, true);

    assertEquals(0, structType.length());
    assertEquals(1, newStructType.length());
  }

  @Test
  public void testIndexOf() {
    StructType structType = new StructType();
    StructType newStructType = structType.add("name", StringType.STRING, true);

    assertEquals(-1, structType.indexOf("name"));
    assertEquals(0, newStructType.indexOf("name"));
  }

  @Test
  public void testGet() {
    StructType structType = new StructType();
    StructType newStructType = structType.add("name", StringType.STRING, true);

    assertThrows(NullPointerException.class, () -> structType.get("name"));
    assertNotNull(newStructType.get("name"));
    assertEquals(StringType.STRING, newStructType.get("name").getDataType());
  }

  @Test
  public void testAt() {
    StructType structType = new StructType();
    StructType newStructType = structType.add("name", StringType.STRING, true);

    assertThrows(IndexOutOfBoundsException.class, () -> structType.at(0));
    assertNotNull(newStructType.at(0));
    assertEquals(StringType.STRING, newStructType.at(0).getDataType());
  }

  @Test
  public void testEquivalent() {
    StructType structType1 = new StructType();
    StructType structType2 = new StructType();

    assertTrue(structType1.equivalent(structType2));

    StructType newStructType1 = structType1.add("name", StringType.STRING, true);
    StructType newStructType2 = structType2.add("name", StringType.STRING, true);
    assertTrue(newStructType1.equivalent(newStructType2));

    StructType newStructType3 = structType1.add("name", IntegerType.INTEGER, true);
    StructType newStructType4 = structType2.add("name", StringType.STRING, true);
    assertFalse(newStructType3.equivalent(newStructType4));
  }

  @Test
  public void testToJson() {
    StructType structType = new StructType();
    StructType newStructType = structType.add("name", StringType.STRING, true);

    assertEquals(
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
        newStructType.toJson());
  }
}
