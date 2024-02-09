package io.whitefox.core;

import io.whitefox.core.services.capabilities.ResponseFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Value;

@Value
public class Metadata {
  String id;
  Optional<String> name;
  Optional<String> description;
  ResponseFormat format;
  TableSchema tableSchema;
  List<String> partitionColumns;
  Map<String, String> configuration;
  Optional<Long> version;
  Optional<Long> size;
  Optional<Long> numFiles;
}
