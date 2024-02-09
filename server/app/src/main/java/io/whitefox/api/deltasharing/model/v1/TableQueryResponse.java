package io.whitefox.api.deltasharing.model.v1;

import io.whitefox.api.deltasharing.model.v1.parquet.ParquetFile;
import io.whitefox.api.deltasharing.model.v1.parquet.ParquetMetadata;
import io.whitefox.api.deltasharing.model.v1.parquet.ParquetProtocol;
import java.util.List;
import lombok.NonNull;
import lombok.Value;

@Value
public class TableQueryResponse {
  @NonNull ParquetProtocol protocol;

  @NonNull ParquetMetadata metadata;

  @NonNull List<ParquetFile> files;
}
