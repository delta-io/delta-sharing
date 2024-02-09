package io.whitefox.core.services.capabilities;

// here because otherwise I could not use it in TableReaderFeatures
public class CapabilitiesConstants {
  public static final String DELTA_SHARING_READER_FEATURE_DELETION_VECTOR = "deletionvectors";
  public static final String DELTA_SHARING_READER_FEATURE_COLUMN_MAPPING = "columnmapping";
  public static final String DELTA_SHARING_READER_FEATURE_TIMESTAMP_NTZ = "timestampntz";
  public static final String DELTA_SHARING_READER_FEATURE_DOMAIN_METADATA = "domainmetadata";
  public static final String DELTA_SHARING_READER_FEATURE_V2CHECKPOINT = "v2checkpoint";
  public static final String DELTA_SHARING_READER_FEATURE_CHECK_CONSTRAINTS = "checkconstraints";
  public static final String DELTA_SHARING_READER_FEATURE_GENERATED_COLUMNS = "generatedcolumns";
  public static final String DELTA_SHARING_READER_FEATURE_ALLOW_COLUMN_DEFAULTS =
      "allowcolumndefaults";
  public static final String DELTA_SHARING_READER_FEATURE_IDENTITY_COLUMNS = "identitycolumns";
  public static final String ICEBERG_V1 = "icebergv1";
  public static final String ICEBERG_V2 = "icebergv2";
}
