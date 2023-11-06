package io.whitefox.core.types;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Objects;

public class MapType extends DataType {

  private final DataType keyType;
  private final DataType valueType;
  private final boolean valueContainsNull;

  public MapType(DataType keyType, DataType valueType, boolean valueContainsNull) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.valueContainsNull = valueContainsNull;
  }

  public DataType getKeyType() {
    return keyType;
  }

  public DataType getValueType() {
    return valueType;
  }

  public boolean isValueContainsNull() {
    return valueContainsNull;
  }

  @Override
  public boolean equivalent(DataType dataType) {
    return dataType instanceof MapType
        && ((MapType) dataType).getKeyType().equivalent(keyType)
        && ((MapType) dataType).getValueType().equivalent(valueType)
        && ((MapType) dataType).valueContainsNull == valueContainsNull;
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
    MapType mapType = (MapType) o;
    return valueContainsNull == mapType.valueContainsNull
        && keyType.equals(mapType.keyType)
        && valueType.equals(mapType.valueType);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(keyType, valueType, valueContainsNull);
  }

  @Override
  public String toJson() {
    return String.format(
        "{" + "\"type\": \"map\","
            + "\"keyType\": %s,"
            + "\"valueType\": %s,"
            + "\"valueContainsNull\": %s"
            + "}",
        keyType.toJson(), valueType.toJson(), valueContainsNull);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return String.format("map[%s, %s]", keyType, valueType);
  }
}
