package io.whitefox.core.types;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class DecimalTypeTest {

  @Test
  public void testConstructor() {
    assertThrows(IllegalArgumentException.class, () -> new DecimalType(-1, 0));
    assertThrows(IllegalArgumentException.class, () -> new DecimalType(0, -1));
    assertThrows(IllegalArgumentException.class, () -> new DecimalType(39, 0));
    assertThrows(IllegalArgumentException.class, () -> new DecimalType(0, 39));
    assertThrows(IllegalArgumentException.class, () -> new DecimalType(2, 3));
    assertDoesNotThrow(() -> new DecimalType(0, 0));
    assertDoesNotThrow(() -> new DecimalType(38, 0));
    assertDoesNotThrow(() -> new DecimalType(38, 38));
  }

  @Test
  public void testGetPrecision() {
    DecimalType decimalType = new DecimalType(10, 2);
    assertEquals(10, decimalType.getPrecision());
  }

  @Test
  public void testGetScale() {
    DecimalType decimalType = new DecimalType(10, 2);
    assertEquals(2, decimalType.getScale());
  }

  @Test
  public void testToJson() {
    DecimalType decimalType = new DecimalType(10, 2);
    assertEquals("\"decimal(10, 2)\"", decimalType.toJson());
  }
}
