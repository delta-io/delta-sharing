package io.whitefox.core.types;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class BasePrimitiveType extends DataType {
  /**
   * Create a primitive type {@link DataType}
   *
   * @param primitiveTypeName Primitive type name.
   * @return
   */
  public static DataType createPrimitive(String primitiveTypeName) {
    return Optional.ofNullable(nameToPrimitiveTypeMap.get().get(primitiveTypeName))
        .orElseThrow(
            () -> new IllegalArgumentException("Unknown primitive type " + primitiveTypeName));
  }

  /**
   * Is the given type name a primitive type?
   */
  public static boolean isPrimitiveType(String typeName) {
    return nameToPrimitiveTypeMap.get().containsKey(typeName);
  }

  /**
   * For testing only
   */
  public static List<DataType> getAllPrimitiveTypes() {
    return nameToPrimitiveTypeMap.get().values().stream().collect(Collectors.toList());
  }

  private static final Supplier<Map<String, DataType>> nameToPrimitiveTypeMap =
      () -> Collections.unmodifiableMap(new HashMap<String, DataType>() {
        {
          put("boolean", BooleanType.BOOLEAN);
          put("byte", ByteType.BYTE);
          put("short", ShortType.SHORT);
          put("integer", IntegerType.INTEGER);
          put("long", LongType.LONG);
          put("float", FloatType.FLOAT);
          put("double", DoubleType.DOUBLE);
          put("date", DateType.DATE);
          put("timestamp", TimestampType.TIMESTAMP);
          put("binary", BinaryType.BINARY);
          put("string", StringType.STRING);
        }
      });

  private final String primitiveTypeName;

  protected BasePrimitiveType(String primitiveTypeName) {
    this.primitiveTypeName = primitiveTypeName;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BasePrimitiveType that = (BasePrimitiveType) o;
    return primitiveTypeName.equals(that.primitiveTypeName);
  }

  @SkipCoverageGenerated
  @Override
  public int hashCode() {
    return Objects.hash(primitiveTypeName);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return primitiveTypeName;
  }

  @Override
  public String toJson() {
    return String.format("\"%s\"", primitiveTypeName);
  }
}
