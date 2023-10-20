package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.List;
import java.util.Objects;

public class ReadTableResultToBeSigned {
  private final Protocol protocol;
  private final Metadata metadata;
  private final List<TableFileToBeSigned> other;

  public ReadTableResultToBeSigned(
      Protocol protocol, Metadata metadata, List<TableFileToBeSigned> other) {
    this.protocol = protocol;
    this.metadata = metadata;
    this.other = other;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ReadTableResultToBeSigned that = (ReadTableResultToBeSigned) o;
    return Objects.equals(protocol, that.protocol)
        && Objects.equals(metadata, that.metadata)
        && Objects.equals(other, that.other);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(protocol, metadata, other);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "QueryTableResult{" + "protocol="
        + protocol + ", metadata="
        + metadata + ", other="
        + other + '}';
  }

  public Protocol protocol() {
    return protocol;
  }

  public Metadata metadata() {
    return metadata;
  }

  public List<TableFileToBeSigned> other() {
    return other;
  }
}
