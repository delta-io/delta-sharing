package io.whitefox.core;

import java.util.List;
import lombok.Value;

@Value
public class ReadTableResult {
  Protocol protocol;
  Metadata metadata;
  List<TableFile> files;
  long version;
}
