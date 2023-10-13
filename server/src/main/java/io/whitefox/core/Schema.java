package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.List;
import java.util.Objects;

public final class Schema {
  private final String name;
  private final List<Table> tables;
  private final String share;

  public Schema(String name, List<Table> tables, String share) {
    this.name = name;
    this.tables = tables;
    this.share = share;
  }

  public String name() {
    return name;
  }

  public List<Table> tables() {
    return tables;
  }

  public String share() {
    return share;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (Schema) obj;
    return Objects.equals(this.name, that.name)
        && Objects.equals(this.tables, that.tables)
        && Objects.equals(this.share, that.share);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(name, tables, share);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "Schema[" + "name=" + name + ", " + "tables=" + tables + ", " + "share=" + share + ']';
  }
}
