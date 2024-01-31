package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public interface ReadTableRequest {

  class ReadTableVersion implements ReadTableRequest {
    private final Optional<List<String>> predicateHints;
    private final Optional<String> jsonPredicateHints;
    private final Optional<Long> limitHint;

    private final Long version;

    public ReadTableVersion(
        Optional<List<String>> predicateHints,
        Optional<String> jsonPredicateHints,
        Optional<Long> limitHint,
        Long version) {

      this.predicateHints = predicateHints;
      this.jsonPredicateHints = jsonPredicateHints;
      this.limitHint = limitHint;
      this.version = version;
    }

    public Optional<String> jsonPredicateHints() {
      return jsonPredicateHints;
    }

    public Optional<List<String>> predicateHints() {
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
          && Objects.equals(jsonPredicateHints, that.jsonPredicateHints)
          && Objects.equals(limitHint, that.limitHint)
          && Objects.equals(version, that.version);
    }

    @Override
    @SkipCoverageGenerated
    public int hashCode() {
      return Objects.hash(predicateHints, jsonPredicateHints, limitHint, version);
    }

    @Override
    @SkipCoverageGenerated
    public String toString() {
      return "ReadTableVersion{" + "predicateHints="
          + predicateHints + "jsonPredicateHints="
          + jsonPredicateHints + ", limitHint="
          + limitHint + ", version="
          + version + '}';
    }
  }

  class ReadTableAsOfTimestamp implements ReadTableRequest {
    private final Optional<List<String>> predicateHints;

    private final Optional<Long> limitHint;
    private final Optional<String> jsonPredicateHints;
    private final Long timestamp;

    public ReadTableAsOfTimestamp(
        Optional<List<String>> predicateHints,
        Optional<String> jsonPredicateHints,
        Optional<Long> limitHint,
        Long timestamp) {

      this.predicateHints = predicateHints;
      this.jsonPredicateHints = jsonPredicateHints;
      this.limitHint = limitHint;
      this.timestamp = timestamp;
    }

    public Optional<String> jsonPredicateHints() {
      return jsonPredicateHints;
    }

    @Override
    @SkipCoverageGenerated
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ReadTableAsOfTimestamp that = (ReadTableAsOfTimestamp) o;
      return Objects.equals(predicateHints, that.predicateHints)
          && Objects.equals(jsonPredicateHints, that.jsonPredicateHints)
          && Objects.equals(limitHint, that.limitHint)
          && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    @SkipCoverageGenerated
    public int hashCode() {
      return Objects.hash(jsonPredicateHints, predicateHints, limitHint, timestamp);
    }

    @Override
    @SkipCoverageGenerated
    public String toString() {
      return "ReadTableAsOfTimestamp{" + "predicateHints="
          + predicateHints + "jsonPredicateHints="
          + jsonPredicateHints + ", limitHint="
          + limitHint + ", timestamp="
          + timestamp + '}';
    }

    public Optional<List<String>> predicateHints() {
      return predicateHints;
    }

    public Optional<Long> limitHint() {
      return limitHint;
    }

    public Long timestamp() {
      return timestamp;
    }
  }

  class ReadTableCurrentVersion implements ReadTableRequest {
    private final Optional<List<String>> predicateHints;
    private final Optional<String> jsonPredicateHints;
    private final Optional<Long> limitHint;

    public ReadTableCurrentVersion(
        Optional<List<String>> predicateHints,
        Optional<String> jsonPredicateHints,
        Optional<Long> limitHint) {
      this.predicateHints = predicateHints;
      this.jsonPredicateHints = jsonPredicateHints;
      this.limitHint = limitHint;
    }

    public Optional<List<String>> predicateHints() {
      return predicateHints;
    }

    public Optional<String> jsonPredicateHints() {
      return jsonPredicateHints;
    }

    public Optional<Long> limitHint() {
      return limitHint;
    }

    @Override
    @SkipCoverageGenerated
    public String toString() {
      return "ReadTableCurrentVersion{" + "predicateHints="
          + predicateHints + "jsonPredicateHints="
          + jsonPredicateHints + ", limitHint="
          + limitHint + '}';
    }

    @Override
    @SkipCoverageGenerated
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ReadTableCurrentVersion that = (ReadTableCurrentVersion) o;
      return Objects.equals(predicateHints, that.predicateHints)
          && Objects.equals(jsonPredicateHints, that.jsonPredicateHints)
          && Objects.equals(limitHint, that.limitHint);
    }

    @Override
    @SkipCoverageGenerated
    public int hashCode() {
      return Objects.hash(jsonPredicateHints, predicateHints, limitHint);
    }
  }
}
