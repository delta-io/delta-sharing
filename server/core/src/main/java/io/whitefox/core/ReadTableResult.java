package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.List;
import java.util.Objects;

public class ReadTableResult {
  private final Protocol protocol;
  private final Metadata metadata;
  private final List<TableFile> files;

  public ReadTableResult(Protocol protocol, Metadata metadata, List<TableFile> files) {
    this.protocol = protocol;
    this.metadata = metadata;
    this.files = files;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ReadTableResult that = (ReadTableResult) o;
    return Objects.equals(protocol, that.protocol)
        && Objects.equals(metadata, that.metadata)
        && Objects.equals(files, that.files);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(protocol, metadata, files);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "QueryTableResult{" + "protocol="
        + protocol + ", metadata="
        + metadata + ", files="
        + files + '}';
  }

  public Protocol protocol() {
    return protocol;
  }

  public Metadata metadata() {
    return metadata;
  }

  public List<TableFile> files() {
    return files;
  }
}
