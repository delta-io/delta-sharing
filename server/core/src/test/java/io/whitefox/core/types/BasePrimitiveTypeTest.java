package io.whitefox.core.types;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BasePrimitiveTypeTest {

  @Test
  public void testCreatePrimitive() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> BasePrimitiveType.createPrimitive("unknown"));
    Assertions.assertEquals(BooleanType.BOOLEAN, BasePrimitiveType.createPrimitive("boolean"));
  }

  @Test
  public void testIsPrimitiveType() {
    Assertions.assertTrue(BasePrimitiveType.isPrimitiveType("boolean"));
    Assertions.assertFalse(BasePrimitiveType.isPrimitiveType("unknown"));
  }

  @Test
  public void testGetAllPrimitiveTypes() {
    Assertions.assertEquals(11, BasePrimitiveType.getAllPrimitiveTypes().size());
  }

  @Test
  public void testEquals() {
    BasePrimitiveType type1 = BooleanType.BOOLEAN;
    BasePrimitiveType type2 = BooleanType.BOOLEAN;
    BasePrimitiveType type3 = IntegerType.INTEGER;
    Assertions.assertEquals(type1, type2);
    Assertions.assertNotEquals(type1, type3);
  }

  @Test
  public void testHashCode() {
    BasePrimitiveType type1 = BooleanType.BOOLEAN;
    BasePrimitiveType type2 = BooleanType.BOOLEAN;
    Assertions.assertEquals(type1.hashCode(), type2.hashCode());
  }

  @Test
  public void testToString() {
    BasePrimitiveType type = BooleanType.BOOLEAN;
    Assertions.assertEquals("boolean", type.toString());
  }

  @Test
  public void testToJson() {
    BasePrimitiveType type = BooleanType.BOOLEAN;
    Assertions.assertEquals("\"boolean\"", type.toJson());
  }
}
