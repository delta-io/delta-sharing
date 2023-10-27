package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Objects;

public class SchemaName {
  private final String name;

  public SchemaName(String name) {
    this.name = name;
  }

  public String name() {
    return name;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SchemaName that = (SchemaName) o;
    return Objects.equals(name, that.name);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return name;
  }
}
