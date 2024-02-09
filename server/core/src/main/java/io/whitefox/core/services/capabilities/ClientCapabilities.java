package io.whitefox.core.services.capabilities;

import java.util.Collections;
import java.util.Set;

public interface ClientCapabilities {
  Set<ReaderFeatures> readerFeatures();

  ResponseFormat responseFormat();

  ParquetClientCapabilities PARQUET_INSTANCE = new ParquetClientCapabilities();

  static DeltaClientCapabilities delta(Set<ReaderFeatures> readerFeatures) {
    return new DeltaClientCapabilities(readerFeatures);
  }

  static ParquetClientCapabilities parquet() {
    return PARQUET_INSTANCE;
  }

  class DeltaClientCapabilities implements ClientCapabilities {
    private final Set<ReaderFeatures> readerFeatures;

    @Override
    public Set<ReaderFeatures> readerFeatures() {
      return readerFeatures;
    }

    @Override
    public ResponseFormat responseFormat() {
      return ResponseFormat.delta;
    }

    private DeltaClientCapabilities(Set<ReaderFeatures> readerFeatures) {
      this.readerFeatures = Collections.unmodifiableSet(readerFeatures);
    }
  }

  class ParquetClientCapabilities implements ClientCapabilities {

    private ParquetClientCapabilities() {}

    @Override
    public Set<ReaderFeatures> readerFeatures() {
      return Set.of();
    }

    @Override
    public ResponseFormat responseFormat() {
      return ResponseFormat.parquet;
    }
  }
}
