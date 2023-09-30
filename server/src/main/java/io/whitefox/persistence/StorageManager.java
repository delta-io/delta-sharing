package io.whitefox.persistence;

import io.whitefox.api.deltasharing.model.Schema;
import io.whitefox.api.deltasharing.model.Share;
import io.whitefox.api.deltasharing.model.Table;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface StorageManager {
  CompletionStage<Optional<Share>> getShare(String share);

  CompletionStage<ResultAndTotalSize<List<Share>>> getShares(int offset, int maxResultSize);

  CompletionStage<Optional<ResultAndTotalSize<List<Schema>>>> listSchemas(
      String share, int offset, int maxResultSize);

  CompletionStage<Optional<ResultAndTotalSize<List<Table>>>> listTables(
      String share, String schema, int offset, int maxResultSize);

  CompletionStage<Optional<ResultAndTotalSize<List<Table>>>> listTablesOfShare(
      String share, int offset, int finalMaxResults);
}
