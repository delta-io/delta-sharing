package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Objects;

public final class Table {
  private final String name;
  private final String location;
  private final String schema;
  private final String share;

  public Table(String name, String location, String schema, String share) {
    this.name = name;
    this.location = location;
    this.schema = schema;
    this.share = share;
  }

  public String name() {
    return name;
  }

  public String location() {
    return location;
  }

  public String schema() {
    return schema;
  }

  public String share() {
    return share;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (Table) obj;
    return Objects.equals(this.name, that.name)
        && Objects.equals(this.location, that.location)
        && Objects.equals(this.schema, that.schema)
        && Objects.equals(this.share, that.share);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(name, location, schema, share);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "Table[" + "name="
        + name + ", " + "location="
        + location + ", " + "schema="
        + schema + ", " + "share="
        + share + ']';
  }
}
