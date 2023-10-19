package io.whitefox.core.types;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ArrayTypeTest {

  @Test
  void getElementType() {
    var array1 = new ArrayType(ShortType.SHORT, false);
    var array2 = new ArrayType(ShortType.SHORT, true);
    var array3 = new ArrayType(StringType.STRING, false);
    assertEquals(array1.getElementType(), array2.getElementType());
    assertNotEquals(array1.getElementType(), array3.getElementType());
    assertNotEquals(array1, array2);
  }

  @Test
  void equivalent() {
    var array1 = new ArrayType(ShortType.SHORT, false);
    var array2 = new ArrayType(ShortType.SHORT, true);
    var array3 = new ArrayType(StringType.STRING, false);
    assertTrue(array1.equivalent(array2));
    assertNotEquals(array1, array2);
    assertFalse(array1.equivalent(array3));
  }

  @Test
  void toJson() {
    var array1 = new ArrayType(ShortType.SHORT, false);
    assertEquals(
        array1.toJson(),
        "{\"type\": \"array\",\"elementType\": \"short\",\"containsNull\": false}");
  }
}
