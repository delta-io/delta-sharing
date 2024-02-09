package io.whitefox.core.services.capabilities;

import static io.whitefox.core.services.capabilities.CapabilitiesConstants.*;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Optional;

@SkipCoverageGenerated
public enum ReaderFeatures {
  DELETION_VECTORS(DELTA_SHARING_READER_FEATURE_DELETION_VECTOR),
  COLUMN_MAPPING(DELTA_SHARING_READER_FEATURE_COLUMN_MAPPING),
  TIMESTAMP_NTZ(DELTA_SHARING_READER_FEATURE_TIMESTAMP_NTZ),
  DOMAIN_METADATA(DELTA_SHARING_READER_FEATURE_DOMAIN_METADATA),
  V2CHECKPOINT(DELTA_SHARING_READER_FEATURE_V2CHECKPOINT),
  CHECK_CONSTRAINTS(DELTA_SHARING_READER_FEATURE_CHECK_CONSTRAINTS),
  GENERATED_COLUMNS(DELTA_SHARING_READER_FEATURE_GENERATED_COLUMNS),
  ALLOW_COLUMN_DEFAULTS(DELTA_SHARING_READER_FEATURE_ALLOW_COLUMN_DEFAULTS),
  IDENTITY_COLUMNS(DELTA_SHARING_READER_FEATURE_IDENTITY_COLUMNS),
  ICEBERG_V1(CapabilitiesConstants.ICEBERG_V1),
  ICEBERG_V2(CapabilitiesConstants.ICEBERG_V2);

  ReaderFeatures(String stringRepresentation) {
    this.stringRepresentation = stringRepresentation;
  }

  private final String stringRepresentation;

  public String stringRepresentation() {
    return stringRepresentation;
  }

  public static Optional<ReaderFeatures> fromString(String s) {
    switch (s.toLowerCase()) {
      case DELTA_SHARING_READER_FEATURE_DELETION_VECTOR:
        return Optional.of(DELETION_VECTORS);
      case DELTA_SHARING_READER_FEATURE_COLUMN_MAPPING:
        return Optional.of(COLUMN_MAPPING);
      case DELTA_SHARING_READER_FEATURE_TIMESTAMP_NTZ:
        return Optional.of(TIMESTAMP_NTZ);
      case DELTA_SHARING_READER_FEATURE_DOMAIN_METADATA:
        return Optional.of(DOMAIN_METADATA);
      case DELTA_SHARING_READER_FEATURE_V2CHECKPOINT:
        return Optional.of(V2CHECKPOINT);
      case DELTA_SHARING_READER_FEATURE_CHECK_CONSTRAINTS:
        return Optional.of(CHECK_CONSTRAINTS);
      case DELTA_SHARING_READER_FEATURE_GENERATED_COLUMNS:
        return Optional.of(GENERATED_COLUMNS);
      case DELTA_SHARING_READER_FEATURE_ALLOW_COLUMN_DEFAULTS:
        return Optional.of(ALLOW_COLUMN_DEFAULTS);
      case DELTA_SHARING_READER_FEATURE_IDENTITY_COLUMNS:
        return Optional.of(IDENTITY_COLUMNS);
      case CapabilitiesConstants.ICEBERG_V1:
        return Optional.of(ICEBERG_V1);
      case CapabilitiesConstants.ICEBERG_V2:
        return Optional.of(ICEBERG_V2);
      default:
        return Optional.empty();
    }
  }
}
