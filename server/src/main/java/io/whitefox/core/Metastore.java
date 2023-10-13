package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Objects;
import java.util.Optional;

public final class Metastore {
  private final String name;
  private final Optional<String> comment;
  private final Principal owner;
  private final MetastoreType type;
  private final MetastoreProperties properties;
  private final Optional<Long> validatedAt;
  private final Long createdAt;
  private final Principal createdBy;
  private final Long updatedAt;
  private final Principal updatedBy;

  public Metastore(
      String name,
      Optional<String> comment,
      Principal owner,
      MetastoreType type,
      MetastoreProperties properties,
      Optional<Long> validatedAt,
      Long createdAt,
      Principal createdBy,
      Long updatedAt,
      Principal updatedBy) {
    this.name = name;
    this.comment = comment;
    this.owner = owner;
    this.type = type;
    this.properties = properties;
    this.validatedAt = validatedAt;
    this.createdAt = createdAt;
    this.createdBy = createdBy;
    this.updatedAt = updatedAt;
    this.updatedBy = updatedBy;
  }

  public String name() {
    return name;
  }

  public Optional<String> comment() {
    return comment;
  }

  public Principal owner() {
    return owner;
  }

  public MetastoreType type() {
    return type;
  }

  public MetastoreProperties properties() {
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

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (Metastore) obj;
    return Objects.equals(this.name, that.name)
        && Objects.equals(this.comment, that.comment)
        && Objects.equals(this.owner, that.owner)
        && Objects.equals(this.type, that.type)
        && Objects.equals(this.properties, that.properties)
        && Objects.equals(this.validatedAt, that.validatedAt)
        && Objects.equals(this.createdAt, that.createdAt)
        && Objects.equals(this.createdBy, that.createdBy)
        && Objects.equals(this.updatedAt, that.updatedAt)
        && Objects.equals(this.updatedBy, that.updatedBy);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(
        name,
        comment,
        owner,
        type,
        properties,
        validatedAt,
        createdAt,
        createdBy,
        updatedAt,
        updatedBy);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "Metastore[" + "name="
        + name + ", " + "comment="
        + comment + ", " + "owner="
        + owner + ", " + "type="
        + type + ", " + "properties="
        + properties + ", " + "validatedAt="
        + validatedAt + ", " + "createdAt="
        + createdAt + ", " + "createdBy="
        + createdBy + ", " + "updatedAt="
        + updatedAt + ", " + "updatedBy="
        + updatedBy + ']';
  }
}
