package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public interface ReadTableRequest {

  public static class ReadTableVersion implements ReadTableRequest {
    private final List<String> predicateHints;
    private final Optional<Long> limitHint;

    private final Long version;

    public ReadTableVersion(List<String> predicateHints, Optional<Long> limitHint, Long version) {
      this.predicateHints = predicateHints;
      this.limitHint = limitHint;
      this.version = version;
    }

    public List<String> predicateHints() {
      return predicateHints;
    }

    public Optional<Long> limitHint() {
      return limitHint;
    }

    public Long version() {
      return version;
    }

    @Override
    @SkipCoverageGenerated
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ReadTableVersion that = (ReadTableVersion) o;
      return Objects.equals(predicateHints, that.predicateHints)
          && Objects.equals(limitHint, that.limitHint)
          && Objects.equals(version, that.version);
    }

    @Override
    @SkipCoverageGenerated
    public int hashCode() {
      return Objects.hash(predicateHints, limitHint, version);
    }

    @Override
    @SkipCoverageGenerated
    public String toString() {
      return "ReadTableVersion{" + "predicateHints="
          + predicateHints + ", limitHint="
          + limitHint + ", version="
          + version + '}';
    }
  }

  public static class ReadTableAsOfTimestamp implements ReadTableRequest {
    private final List<String> predicateHints;
    private final Optional<Long> limitHint;
    private final Long timestamp;

    public ReadTableAsOfTimestamp(
        List<String> predicateHints, Optional<Long> limitHint, Long timestamp) {
      this.predicateHints = predicateHints;
      this.limitHint = limitHint;
      this.timestamp = timestamp;
    }

    @Override
    @SkipCoverageGenerated
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ReadTableAsOfTimestamp that = (ReadTableAsOfTimestamp) o;
      return Objects.equals(predicateHints, that.predicateHints)
          && Objects.equals(limitHint, that.limitHint)
          && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    @SkipCoverageGenerated
    public int hashCode() {
      return Objects.hash(predicateHints, limitHint, timestamp);
    }

    @Override
    @SkipCoverageGenerated
    public String toString() {
      return "ReadTableAsOfTimestamp{" + "predicateHints="
          + predicateHints + ", limitHint="
          + limitHint + ", timestamp="
          + timestamp + '}';
    }

    public List<String> predicateHints() {
      return predicateHints;
    }

    public Optional<Long> limitHint() {
      return limitHint;
    }

    public Long timestamp() {
      return timestamp;
    }
  }

  public static class ReadTableCurrentVersion implements ReadTableRequest {
    private final List<String> predicateHints;
    private final Optional<Long> limitHint;

    public ReadTableCurrentVersion(List<String> predicateHints, Optional<Long> limitHint) {
      this.predicateHints = predicateHints;
      this.limitHint = limitHint;
    }

    public List<String> predicateHints() {
      return predicateHints;
    }

    public Optional<Long> limitHint() {
      return limitHint;
    }

    @Override
    @SkipCoverageGenerated
    public String toString() {
      return "ReadTableCurrentVersion{" + "predicateHints="
          + predicateHints + ", limitHint="
          + limitHint + '}';
    }

    @Override
    @SkipCoverageGenerated
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ReadTableCurrentVersion that = (ReadTableCurrentVersion) o;
      return Objects.equals(predicateHints, that.predicateHints)
          && Objects.equals(limitHint, that.limitHint);
    }

    @Override
    @SkipCoverageGenerated
    public int hashCode() {
      return Objects.hash(predicateHints, limitHint);
    }
  }
}
