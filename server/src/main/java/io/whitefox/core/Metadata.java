package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.List;
import java.util.Objects;

public class Metadata {
  private final String id;
  private final Format format;
  private final TableSchema tableSchema;
  private final List<String> partitionColumns;

  public enum Format {
    PARQUET("parquet");

    private final String provider;

    private Format(final String provider) {
      this.provider = provider;
    }

    public String provider() {
      return this.provider;
    }
  }

  public Metadata(
      String id, Format format, TableSchema tableSchema, List<String> partitionColumns) {
    this.id = id;
    this.format = format;
    this.tableSchema = tableSchema;
    this.partitionColumns = partitionColumns;
  }

  public String id() {
    return id;
  }

  public Format format() {
    return format;
  }

  public TableSchema tableSchema() {
    return tableSchema;
  }

  public List<String> partitionColumns() {
    return partitionColumns;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Metadata metadata = (Metadata) o;
    return Objects.equals(id, metadata.id)
        && format == metadata.format
        && Objects.equals(tableSchema, metadata.tableSchema)
        && Objects.equals(partitionColumns, metadata.partitionColumns);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(id, format, tableSchema, partitionColumns);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "Metadata{" + "id='"
        + id + '\'' + ", format="
        + format + ", tableSchema="
        + tableSchema + ", partitionColumns="
        + partitionColumns + '}';
  }
}
