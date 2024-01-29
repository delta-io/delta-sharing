package io.whitefox.core;

import java.util.List;
import lombok.Value;

@Value
public class ReadTableResultToBeSigned {
  Protocol protocol;
  Metadata metadata;
  List<TableFileToBeSigned> other;
  long version;
}
