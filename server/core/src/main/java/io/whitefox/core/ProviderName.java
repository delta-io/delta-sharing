package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Objects;

public class ProviderName {
  private final String name;

  public ProviderName(String share) {
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
    ProviderName that = (ProviderName) o;
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
