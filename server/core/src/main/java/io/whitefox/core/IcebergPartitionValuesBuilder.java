package io.whitefox.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;

public class IcebergPartitionValuesBuilder {

  public Map<String, String> buildPartitionValues(
      List<Types.NestedField> partitionFields, StructLike partitionValues) {
    var map = new HashMap<String, String>();
    for (int i = 0; i < partitionFields.size(); i++) {
      Types.NestedField field = partitionFields.get(i);
      map.put(
          field.name(),
          partitionValues.get(i, field.type().typeId().javaClass()).toString());
    }
    return map;
  }
}
