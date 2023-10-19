package io.whitefox.core.types;

public abstract class DataType {
  /**
   * Convert the data type to Delta protocol specified serialization format.
   *
   * @return Data type serialized in JSON format.
   */
  public abstract String toJson();

  /**
   * Are the data types same? The metadata or column names could be different.
   *
   * @param dataType
   * @return
   */
  public boolean equivalent(DataType dataType) {
    return equals(dataType);
  }

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract String toString();
}
