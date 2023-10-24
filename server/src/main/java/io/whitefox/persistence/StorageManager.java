package io.whitefox.persistence;

import io.whitefox.core.*;
import io.whitefox.core.Storage;
import java.util.List;
import java.util.Optional;

public interface StorageManager {
  Optional<Share> getShare(String share);

  ResultAndTotalSize<List<Share>> getShares(int offset, int maxResultSize);

  Optional<SharedTable> getSharedTable(String share, String schema, String table);

  Optional<ResultAndTotalSize<List<Schema>>> listSchemas(
      String share, int offset, int maxResultSize);

  Optional<ResultAndTotalSize<List<SharedTable>>> listTables(
      String share, String schema, int offset, int maxResultSize);

  Optional<ResultAndTotalSize<List<SharedTable>>> listTablesOfShare(
      String share, int offset, int finalMaxResults);

  Metastore createMetastore(Metastore metastore);

  Optional<Metastore> getMetastore(String name);

  Storage createStorage(Storage storage);

  Optional<Storage> getStorage(String name);

  Provider createProvider(Provider provider);

  Optional<Provider> getProvider(String name);

  InternalTable createInternalTable(InternalTable internalTable);
}
