package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Objects;

public final class Principal {
  private final String name;

  public Principal(String name) {
    this.name = name;
  }

  public String name() {
    return name;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (Principal) obj;
    return Objects.equals(this.name, that.name);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "Principal[" + "name=" + name + ']';
  }
}
