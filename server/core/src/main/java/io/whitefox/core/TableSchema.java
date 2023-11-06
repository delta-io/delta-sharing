package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import io.whitefox.core.types.StructType;
import java.util.Objects;

public class TableSchema {
  private final StructType structType;

  public TableSchema(StructType structType) {
    this.structType = structType;
  }

  public StructType structType() {
    return structType;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TableSchema that = (TableSchema) o;
    return Objects.equals(structType, that.structType);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(structType);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "TableSchema{" + "structType=" + structType + '}';
  }
}
