package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class Schema {
  private final String name;
  private final Map<String, SharedTable> sharedTables;
  private final String share;

  public Schema(String name, Collection<SharedTable> sharedTables, String share) {
    this(
        name,
        sharedTables.stream().collect(Collectors.toMap(SharedTable::name, Function.identity())),
        share);
  }

  public Schema(String name, Map<String, SharedTable> sharedTables, String share) {
    this.name = name;
    this.sharedTables = sharedTables;
    this.share = share;
  }

  public String name() {
    return name;
  }

  public Set<SharedTable> tables() {
    return Set.copyOf(sharedTables.values());
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
        && Objects.equals(this.sharedTables, that.sharedTables)
        && Objects.equals(this.share, that.share);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(name, sharedTables, share);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "Schema[" + "name=" + name + ", " + "tables=" + sharedTables + ", " + "share=" + share
        + ']';
  }

  public Schema addTable(InternalTable table, SharedTableName sharedTableName) {
    var newSharedTables = new HashMap<>(sharedTables);
    newSharedTables.put(
        sharedTableName.name(), new SharedTable(sharedTableName.name(), this.name, share, table));
    return new Schema(this.name, newSharedTables, share);
  }
}
