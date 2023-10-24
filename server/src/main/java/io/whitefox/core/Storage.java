package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Objects;
import java.util.Optional;

public final class Storage {
  private final String name;
  private final Optional<String> comment;
  private final Principal owner;
  private final StorageType type;
  private final Optional<Long> validatedAt;
  private final String uri;
  private final Long createdAt;
  private final Principal createdBy;
  private final Long updatedAt;
  private final Principal updatedBy;
  private final StorageProperties properties;

  public Storage(
      String name,
      Optional<String> comment,
      Principal owner,
      StorageType type,
      Optional<Long> validatedAt,
      String uri,
      Long createdAt,
      Principal createdBy,
      Long updatedAt,
      Principal updatedBy,
      StorageProperties properties) {
    this.name = name;
    this.comment = comment;
    this.owner = owner;
    this.type = type;
    this.validatedAt = validatedAt;
    this.uri = uri;
    this.createdAt = createdAt;
    this.createdBy = createdBy;
    this.updatedAt = updatedAt;
    this.updatedBy = updatedBy;
    this.properties = properties;
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

  public StorageType type() {
    return type;
  }

  public Optional<Long> validatedAt() {
    return validatedAt;
  }

  public String uri() {
    return uri;
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

  public StorageProperties properties() {
    return properties;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Storage storage = (Storage) o;
    return Objects.equals(name, storage.name)
        && Objects.equals(comment, storage.comment)
        && Objects.equals(owner, storage.owner)
        && type == storage.type
        && Objects.equals(validatedAt, storage.validatedAt)
        && Objects.equals(uri, storage.uri)
        && Objects.equals(createdAt, storage.createdAt)
        && Objects.equals(createdBy, storage.createdBy)
        && Objects.equals(updatedAt, storage.updatedAt)
        && Objects.equals(updatedBy, storage.updatedBy)
        && Objects.equals(properties, storage.properties);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(
        name,
        comment,
        owner,
        type,
        validatedAt,
        uri,
        createdAt,
        createdBy,
        updatedAt,
        updatedBy,
        properties);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "Storage{" + "name='"
        + name + '\'' + ", comment="
        + comment + ", owner="
        + owner + ", type="
        + type + ", validatedAt="
        + validatedAt + ", uri='"
        + uri + '\'' + ", createdAt="
        + createdAt + ", createdBy="
        + createdBy + ", updatedAt="
        + updatedAt + ", updatedBy="
        + updatedBy + ", properties="
        + properties + '}';
  }
}
