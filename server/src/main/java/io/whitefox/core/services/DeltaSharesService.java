package io.whitefox.core.services;

import io.whitefox.core.*;
import io.whitefox.core.Metadata;
import io.whitefox.core.Schema;
import io.whitefox.core.Share;
import io.whitefox.core.SharedTable;
import java.util.List;
import java.util.Optional;

public interface DeltaSharesService {

  Optional<Long> getTableVersion(
      String share, String schema, String table, String startingTimestamp);

  ContentAndToken<List<Share>> listShares(
      Optional<ContentAndToken.Token> nextPageToken, Optional<Integer> maxResults);

  Optional<Metadata> getTableMetadata(
      String share, String schema, String table, String startingTimestamp);

  Optional<ContentAndToken<List<Schema>>> listSchemas(
      String share, Optional<ContentAndToken.Token> nextPageToken, Optional<Integer> maxResults);

  Optional<ContentAndToken<List<SharedTable>>> listTables(
      String share,
      String schema,
      Optional<ContentAndToken.Token> nextPageToken,
      Optional<Integer> maxResults);

  Optional<ContentAndToken<List<SharedTable>>> listTablesOfShare(
      String share, Optional<ContentAndToken.Token> token, Optional<Integer> maxResults);

  Optional<ReadTableResult> queryTable(
      String share, String schema, String table, ReadTableRequest queryRequest);
}
