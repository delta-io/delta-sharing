package io.whitefox.persistence;

import io.whitefox.core.*;
import java.util.List;
import java.util.Optional;

public interface StorageManager {
  Optional<Share> getShare(String share);

  ResultAndTotalSize<List<Share>> getShares(int offset, int maxResultSize);

  Optional<Table> getTable(String share, String schema, String table);

  Optional<ResultAndTotalSize<List<Schema>>> listSchemas(
      String share, int offset, int maxResultSize);

  Optional<ResultAndTotalSize<List<Table>>> listTables(
      String share, String schema, int offset, int maxResultSize);

  Optional<ResultAndTotalSize<List<Table>>> listTablesOfShare(
      String share, int offset, int finalMaxResults);

  Metastore createMetastore(Metastore metastore);

  Optional<Metastore> getMetastore(String name);
}
