package io.whitefox.api.deltasharing.model;

import io.delta.standalone.actions.Metadata;
import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Objects;

public class DeltaTableMetadata {
  private final long tableVersion;
  private final Metadata metadata;

  public DeltaTableMetadata(long tableVersion, Metadata metadata) {
    this.tableVersion = tableVersion;
    this.metadata = metadata;
  }

  public long getTableVersion() {
    return tableVersion;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DeltaTableMetadata that = (DeltaTableMetadata) o;

    if (tableVersion != that.tableVersion) return false;
    return Objects.equals(metadata, that.metadata);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    int result = (int) (tableVersion ^ (tableVersion >>> 32));
    result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
    return result;
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "DeltaTableMetadata{" + "tableVersion=" + tableVersion + ", metadata=" + metadata + '}';
  }
}
