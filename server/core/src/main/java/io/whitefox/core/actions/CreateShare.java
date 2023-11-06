package io.whitefox.core.actions;

import io.whitefox.annotations.SkipCoverageGenerated;
import io.whitefox.core.Principal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class CreateShare {
  private final String name;
  private final Optional<String> comment;
  private final List<Principal> recipients;
  private final List<String> schemas;

  public CreateShare(
      String name, Optional<String> comment, List<Principal> recipients, List<String> schemas) {
    this.name = name;
    this.comment = comment;
    this.recipients = recipients;
    this.schemas = schemas;
  }

  public String name() {
    return name;
  }

  public Optional<String> comment() {
    return comment;
  }

  public List<Principal> recipients() {
    return recipients;
  }

  public List<String> schemas() {
    return schemas;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateShare that = (CreateShare) o;
    return Objects.equals(name, that.name)
        && Objects.equals(comment, that.comment)
        && Objects.equals(recipients, that.recipients)
        && Objects.equals(schemas, that.schemas);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(name, comment, recipients, schemas);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "CreateShare{" + "name='"
        + name + '\'' + ", comment="
        + comment + ", recipients="
        + recipients + ", schemas="
        + schemas + '}';
  }
}
