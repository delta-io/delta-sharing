package io.whitefox.core.types;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class StructField {

  ////////////////////////////////////////////////////////////////////////////////
  // Static Fields / Methods
  ////////////////////////////////////////////////////////////////////////////////

  /**
   * Indicates a metadata column when present in the field metadata and the value is true
   */
  private static String IS_METADATA_COLUMN_KEY = "isMetadataColumn";

  /**
   * The name of a row index metadata column. When present this column must be populated with
   * row index of each row when reading from parquet.
   */
  public static String METADATA_ROW_INDEX_COLUMN_NAME = "_metadata.row_index";

  public static StructField METADATA_ROW_INDEX_COLUMN = new StructField(
      METADATA_ROW_INDEX_COLUMN_NAME,
      LongType.LONG,
      false,
      Collections.singletonMap(IS_METADATA_COLUMN_KEY, "true"));

  ////////////////////////////////////////////////////////////////////////////////
  // Instance Fields / Methods
  ////////////////////////////////////////////////////////////////////////////////

  private final String name;
  private final DataType dataType;
  private final boolean nullable;
  private final Map<String, String> metadata;

  public StructField(
      String name, DataType dataType, boolean nullable, Map<String, String> metadata) {
    this.name = name;
    this.dataType = dataType;
    this.nullable = nullable;
    this.metadata = metadata;
  }

  /**
   * @return the name of this field
   */
  public String getName() {
    return name;
  }

  /**
   * @return the data type of this field
   */
  public DataType getDataType() {
    return dataType;
  }

  /**
   * @return the metadata for this field
   */
  public Map<String, String> getMetadata() {
    return metadata;
  }

  /**
   * @return whether this field allows to have a {@code null} value.
   */
  public boolean isNullable() {
    return nullable;
  }

  public boolean isMetadataColumn() {
    return metadata.containsKey(IS_METADATA_COLUMN_KEY)
        && Boolean.parseBoolean(metadata.get(IS_METADATA_COLUMN_KEY));
  }

  public boolean isDataColumn() {
    return !isMetadataColumn();
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return String.format(
        "StructField(name=%s,type=%s,nullable=%s,metadata=%s)",
        name, dataType, nullable, "empty(fix - this)");
  }

  public String toJson() {
    String metadataAsJson = metadata.entrySet().stream()
        .map(e -> String.format("\"%s\" : \"%s\"", e.getKey(), e.getValue()))
        .collect(Collectors.joining(",\n"));

    return String.format(
        "{\"name\":\"%s\"," + "\"type\":%s," + "\"nullable\":%s," + "\"metadata\":{%s}" + "}",
        name, dataType.toJson(), nullable, metadataAsJson);
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StructField that = (StructField) o;
    return nullable == that.nullable
        && name.equals(that.name)
        && dataType.equals(that.dataType)
        && metadata.equals(that.metadata);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(name, dataType, nullable, metadata);
  }
}
