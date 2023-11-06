package io.whitefox.core.actions;

import io.whitefox.annotations.SkipCoverageGenerated;
import io.whitefox.core.InternalTable;
import java.util.Objects;
import java.util.Optional;

public class CreateInternalTable {
  private final String name;
  private final Optional<String> comment;
  private final Boolean skipValidation;
  private final InternalTable.InternalTableProperties properties;

  public CreateInternalTable(
      String name,
      Optional<String> comment,
      Boolean skipValidation,
      InternalTable.InternalTableProperties properties) {
    this.name = name;
    this.comment = comment;
    this.skipValidation = skipValidation;
    this.properties = properties;
  }

  public String name() {
    return name;
  }

  public Optional<String> comment() {
    return comment;
  }

  public Boolean skipValidation() {
    return skipValidation;
  }

  public InternalTable.InternalTableProperties properties() {
    return properties;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateInternalTable that = (CreateInternalTable) o;
    return Objects.equals(name, that.name)
        && Objects.equals(comment, that.comment)
        && Objects.equals(skipValidation, that.skipValidation)
        && Objects.equals(properties, that.properties);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(name, comment, skipValidation, properties);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "CreateInternalTable{" + "name='"
        + name + '\'' + ", comment='"
        + comment + '\'' + ", skipValidation="
        + skipValidation + ", properties="
        + properties + '}';
  }
}
