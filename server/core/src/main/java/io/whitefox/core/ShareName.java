package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Objects;

public final class ShareName {

  private final String name;

  public ShareName(String share) {
    this.name = share;
  }

  public String name() {
    return name;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ShareName shareName = (ShareName) o;
    return Objects.equals(name, shareName.name);
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
