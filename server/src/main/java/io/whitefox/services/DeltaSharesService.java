package io.whitefox.services;

import io.whitefox.api.deltasharing.model.Schema;
import io.whitefox.api.deltasharing.model.Share;
import io.whitefox.api.deltasharing.model.Table;
import java.util.List;
import java.util.Optional;

public interface DeltaSharesService {

  Optional<Share> getShare(String share);

  Optional<Long> getTableVersion(
      String share, String schema, String table, String startingTimestamp);

  ContentAndToken<List<Share>> listShares(
      Optional<ContentAndToken.Token> nextPageToken, Optional<Integer> maxResults);

  Optional<ContentAndToken<List<Schema>>> listSchemas(
      String share, Optional<ContentAndToken.Token> nextPageToken, Optional<Integer> maxResults);

  Optional<ContentAndToken<List<Table>>> listTables(
      String share,
      String schema,
      Optional<ContentAndToken.Token> nextPageToken,
      Optional<Integer> maxResults);

  Optional<ContentAndToken<List<Table>>> listTablesOfShare(
      String share, Optional<ContentAndToken.Token> token, Optional<Integer> maxResults);
}
