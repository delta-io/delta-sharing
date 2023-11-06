package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class Metadata {
  private final String id;
  private final Optional<String> name;
  private final Optional<String> description;
  private final Format format;
  private final TableSchema tableSchema;
  private final List<String> partitionColumns;
  private final Map<String, String> configuration;
  private final Optional<Long> version;
  private final Optional<Long> size;
  private final Optional<Long> numFiles;

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
      String id,
      Optional<String> name,
      Optional<String> description,
      Format format,
      TableSchema tableSchema,
      List<String> partitionColumns,
      Map<String, String> configuration,
      Optional<Long> version,
      Optional<Long> size,
      Optional<Long> numFiles) {
    this.id = id;
    this.name = name;
    this.description = description;
    this.format = format;
    this.tableSchema = tableSchema;
    this.partitionColumns = partitionColumns;
    this.configuration = configuration;
    this.version = version;
    this.size = size;
    this.numFiles = numFiles;
  }

  public String id() {
    return id;
  }

  public Optional<String> name() {
    return name;
  }

  public Optional<String> description() {
    return description;
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

  public Map<String, String> configuration() {
    return configuration;
  }

  public Optional<Long> version() {
    return version;
  }

  public Optional<Long> size() {
    return size;
  }

  public Optional<Long> numFiles() {
    return numFiles;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Metadata metadata = (Metadata) o;
    return Objects.equals(id, metadata.id)
        && Objects.equals(name, metadata.name)
        && Objects.equals(description, metadata.description)
        && format == metadata.format
        && Objects.equals(tableSchema, metadata.tableSchema)
        && Objects.equals(partitionColumns, metadata.partitionColumns)
        && Objects.equals(configuration, metadata.configuration)
        && Objects.equals(version, metadata.version)
        && Objects.equals(size, metadata.size)
        && Objects.equals(numFiles, metadata.numFiles);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(
        id,
        name,
        description,
        format,
        tableSchema,
        partitionColumns,
        configuration,
        version,
        size,
        numFiles);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "Metadata{" + "id='"
        + id + '\'' + ", name="
        + name + ", description="
        + description + ", format="
        + format + ", tableSchema="
        + tableSchema + ", partitionColumns="
        + partitionColumns + ", configuration="
        + configuration + ", version="
        + version + ", size="
        + size + ", numFiles="
        + numFiles + '}';
  }
}
