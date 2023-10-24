package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class InternalTable {
  private final String name;
  private final Optional<String> comment;
  private final InternalTableProperties properties;
  private final Optional<Long> validatedAt;
  private final Long createdAt;
  private final Principal createdBy;
  private final Long updatedAt;
  private final Principal updatedBy;
  private final Provider provider;

  public InternalTable(
      String name,
      Optional<String> comment,
      InternalTableProperties properties,
      Optional<Long> validatedAt,
      Long createdAt,
      Principal createdBy,
      Long updatedAt,
      Principal updatedBy,
      Provider provider) {
    this.name = name;
    this.comment = comment;
    this.properties = properties;
    this.validatedAt = validatedAt;
    this.createdAt = createdAt;
    this.createdBy = createdBy;
    this.updatedAt = updatedAt;
    this.updatedBy = updatedBy;
    this.provider = provider;
  }

  public String name() {
    return name;
  }

  public Optional<String> comment() {
    return comment;
  }

  public InternalTableProperties properties() {
    return properties;
  }

  public Optional<Long> validatedAt() {
    return validatedAt;
  }

  public Long createdAt() {
    return createdAt;
  }

  public Principal createdBy() {
    return createdBy;
  }

  public Long updatedAt() {
    return updatedAt;
  }

  public Principal updatedBy() {
    return updatedBy;
  }

  public Provider provider() {
    return provider;
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "InternalTable{" + "name='"
        + name + '\'' + ", comment="
        + comment + ", properties="
        + properties + ", validatedAt="
        + validatedAt + ", createdAt="
        + createdAt + ", createdBy="
        + createdBy + ", updatedAt="
        + updatedAt + ", updatedBy="
        + updatedBy + ", provider="
        + provider + '}';
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    InternalTable that = (InternalTable) o;
    return Objects.equals(name, that.name)
        && Objects.equals(comment, that.comment)
        && Objects.equals(properties, that.properties)
        && Objects.equals(validatedAt, that.validatedAt)
        && Objects.equals(createdAt, that.createdAt)
        && Objects.equals(createdBy, that.createdBy)
        && Objects.equals(updatedAt, that.updatedAt)
        && Objects.equals(updatedBy, that.updatedBy)
        && Objects.equals(provider, that.provider);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(
        name,
        comment,
        properties,
        validatedAt,
        createdAt,
        createdBy,
        updatedAt,
        updatedBy,
        provider);
  }

  public interface InternalTableProperties {
    Map<String, String> asMap();

    static InternalTableProperties fromMap(Map<String, String> map) {
      String type = map.get("type");
      switch (type) {
        case IcebergTableProperties.type:
          return new IcebergTableProperties(map.get("databaseName"), map.get("tableName"));
        case DeltaTableProperties.type:
          return new DeltaTableProperties(map.get("location"));
        default:
          throw new IllegalArgumentException("Unknown type: " + type);
      }
    }
  }

  public static class IcebergTableProperties implements InternalTableProperties {

    public static final String type = "iceberg";
    private final String databaseName;
    private final String tableName;

    public IcebergTableProperties(String databaseName, String tableName) {
      this.databaseName = databaseName;
      this.tableName = tableName;
    }

    @Override
    @SkipCoverageGenerated
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      IcebergTableProperties that = (IcebergTableProperties) o;
      return Objects.equals(databaseName, that.databaseName)
          && Objects.equals(tableName, that.tableName);
    }

    @Override
    @SkipCoverageGenerated
    public int hashCode() {
      return Objects.hash(databaseName, tableName);
    }

    @Override
    @SkipCoverageGenerated
    public String toString() {
      return "IcebergTableProperties{" + "databaseName='"
          + databaseName + '\'' + ", tableName='"
          + tableName + '\'' + '}';
    }

    public String databaseName() {
      return databaseName;
    }

    public String tableName() {
      return tableName;
    }

    @Override
    public Map<String, String> asMap() {
      return Map.of(
          "type", type,
          "databaseName", databaseName,
          "tableName", tableName);
    }
  }

  public static class DeltaTableProperties implements InternalTableProperties {

    public static final String type = "delta";
    private final String location;

    public DeltaTableProperties(String location) {
      this.location = location;
    }

    public String location() {
      return location;
    }

    @Override
    @SkipCoverageGenerated
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DeltaTableProperties that = (DeltaTableProperties) o;
      return Objects.equals(location, that.location);
    }

    @Override
    @SkipCoverageGenerated
    public int hashCode() {
      return Objects.hash(location);
    }

    @Override
    @SkipCoverageGenerated
    public String toString() {
      return "DeltaTableProperties{" + "location='" + location + '\'' + '}';
    }

    @Override
    public Map<String, String> asMap() {
      return Map.of(
          "type", type,
          "location", location);
    }
  }
}
