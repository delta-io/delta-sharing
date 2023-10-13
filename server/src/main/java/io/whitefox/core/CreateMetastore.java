package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Objects;
import java.util.Optional;

public final class CreateMetastore {
  private final String name;
  private final Optional<String> comment;
  private final MetastoreType type;
  private final MetastoreProperties properties;
  private final Principal currentUser;

  private final boolean skipValidation;

  public CreateMetastore(
      String name,
      Optional<String> comment,
      MetastoreType type,
      MetastoreProperties properties,
      Principal currentUser,
      boolean skipValidation) {
    this.name = name;
    this.comment = comment;
    this.type = type;
    this.properties = properties;
    this.currentUser = currentUser;
    this.skipValidation = skipValidation;
  }

  public String name() {
    return name;
  }

  public Optional<String> comment() {
    return comment;
  }

  public MetastoreType type() {
    return type;
  }

  public MetastoreProperties properties() {
    return properties;
  }

  public Principal currentUser() {
    return currentUser;
  }

  public boolean skipValidation() {
    return skipValidation;
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "CreateMetastore{" + "name='"
        + name + '\'' + ", comment="
        + comment + ", type="
        + type + ", properties="
        + properties + ", currentUser="
        + currentUser + ", skipValidation="
        + skipValidation + '}';
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateMetastore that = (CreateMetastore) o;
    return skipValidation == that.skipValidation
        && Objects.equals(name, that.name)
        && Objects.equals(comment, that.comment)
        && type == that.type
        && Objects.equals(properties, that.properties)
        && Objects.equals(currentUser, that.currentUser);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(name, comment, type, properties, currentUser, skipValidation);
  }
}
