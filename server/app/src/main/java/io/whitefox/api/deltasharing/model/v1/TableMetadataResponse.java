package io.whitefox.api.deltasharing.model.v1;

import io.whitefox.api.deltasharing.model.v1.parquet.ParquetMetadata;
import io.whitefox.api.deltasharing.model.v1.parquet.ParquetProtocol;
import lombok.NonNull;
import lombok.Value;

@Value
public class TableMetadataResponse {
  @NonNull ParquetProtocol protocol;

  @NonNull ParquetMetadata metadata;
}
