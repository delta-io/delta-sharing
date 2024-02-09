package io.whitefox.core;

import io.whitefox.core.services.capabilities.ResponseFormat;
import java.util.List;
import lombok.Value;

@Value
public class ReadTableResult {
  Protocol protocol;
  Metadata metadata;
  List<TableFile> files;
  long version;
  ResponseFormat responseFormat;
}
