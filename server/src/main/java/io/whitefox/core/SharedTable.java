package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Objects;

public final class SharedTable {
  private final String name;
  private final String schema;
  private final String share;
  private final InternalTable internalTable;

  public SharedTable(String name, String schema, String share, InternalTable internalTable) {
    this.name = name;
    this.schema = schema;
    this.share = share;
    this.internalTable = internalTable;
  }

  public String name() {
    return name;
  }

  public String schema() {
    return schema;
  }

  public String share() {
    return share;
  }

  public InternalTable internalTable() {
    return internalTable;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SharedTable that = (SharedTable) o;
    return Objects.equals(name, that.name)
        && Objects.equals(schema, that.schema)
        && Objects.equals(share, that.share)
        && Objects.equals(internalTable, that.internalTable);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(name, schema, share, internalTable);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "SharedTable{" + "name='"
        + name + '\'' + ", schema='"
        + schema + '\'' + ", share='"
        + share + '\'' + ", internalTable="
        + internalTable + '}';
  }
}
