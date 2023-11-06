package io.whitefox.core.types;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import scala.Tuple2;

public final class StructType extends DataType {

  private final Map<String, Tuple2<StructField, Integer>> nameToFieldAndOrdinal;
  private final List<StructField> fields;
  private final List<String> fieldNames;

  public StructType() {
    this(new ArrayList<>());
  }

  public StructType(List<StructField> fields) {
    this.fields = fields;
    this.fieldNames = fields.stream().map(f -> f.getName()).collect(Collectors.toList());

    this.nameToFieldAndOrdinal = new HashMap<>();
    for (int i = 0; i < fields.size(); i++) {
      nameToFieldAndOrdinal.put(fields.get(i).getName(), new Tuple2<>(fields.get(i), i));
    }
  }

  public StructType add(StructField field) {
    final List<StructField> fieldsCopy = new ArrayList<>(fields);
    fieldsCopy.add(field);

    return new StructType(fieldsCopy);
  }

  public StructType add(String name, DataType dataType) {
    return add(new StructField(name, dataType, true /* nullable */, new HashMap<>()));
  }

  public StructType add(String name, DataType dataType, boolean nullable) {
    return add(new StructField(name, dataType, nullable, new HashMap<>()));
  }

  public StructType add(String name, DataType dataType, Map<String, String> metadata) {
    return add(new StructField(name, dataType, true /* nullable */, metadata));
  }

  /**
   * @return array of fields
   */
  public List<StructField> fields() {
    return Collections.unmodifiableList(fields);
  }

  /**
   * @return array of field names
   */
  public List<String> fieldNames() {
    return fieldNames;
  }

  /**
   * @return the number of fields
   */
  public int length() {
    return fields.size();
  }

  public int indexOf(String fieldName) {
    return fieldNames.indexOf(fieldName);
  }

  public StructField get(String fieldName) {
    return nameToFieldAndOrdinal.get(fieldName)._1;
  }

  public StructField at(int index) {
    return fields.get(index);
  }

  @Override
  public boolean equivalent(DataType dataType) {
    if (!(dataType instanceof StructType)) {
      return false;
    }

    StructType otherType = ((StructType) dataType);
    return otherType.length() == length()
        && IntStream.range(0, length())
            .mapToObj(i -> otherType.at(i).getDataType().equivalent(at(i).getDataType()))
            .allMatch(result -> result);
  }

  @Override
  public String toString() {
    return String.format(
        "struct(%s)", fields.stream().map(StructField::toString).collect(Collectors.joining(", ")));
  }

  @Override
  public String toJson() {
    String fieldsAsJson = fields.stream().map(e -> e.toJson()).collect(Collectors.joining(",\n"));

    return String.format("{\"type\":\"struct\",\"fields\":[%s]}", fieldsAsJson);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StructType that = (StructType) o;
    return nameToFieldAndOrdinal.equals(that.nameToFieldAndOrdinal)
        && fields.equals(that.fields)
        && fieldNames.equals(that.fieldNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nameToFieldAndOrdinal, fields, fieldNames);
  }
}
