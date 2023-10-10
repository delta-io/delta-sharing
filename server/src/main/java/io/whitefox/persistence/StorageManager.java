package io.whitefox.persistence;

import io.whitefox.persistence.memory.PSchema;
import io.whitefox.persistence.memory.PShare;
import io.whitefox.persistence.memory.PTable;
import java.util.List;
import java.util.Optional;

public interface StorageManager {
  Optional<PShare> getShare(String share);

  ResultAndTotalSize<List<PShare>> getShares(int offset, int maxResultSize);

  Optional<PTable> getTable(String share, String schema, String table);

  Optional<ResultAndTotalSize<List<PSchema>>> listSchemas(
      String share, int offset, int maxResultSize);

  Optional<ResultAndTotalSize<List<PTable>>> listTables(
      String share, String schema, int offset, int maxResultSize);

  Optional<ResultAndTotalSize<List<PTable>>> listTablesOfShare(
      String share, int offset, int finalMaxResults);
}
