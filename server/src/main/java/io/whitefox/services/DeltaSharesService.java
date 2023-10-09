package io.whitefox.services;

import io.whitefox.api.deltasharing.model.Schema;
import io.whitefox.api.deltasharing.model.Share;
import io.whitefox.api.deltasharing.model.Table;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface DeltaSharesService {

  CompletionStage<Optional<Share>> getShare(String share);

  CompletionStage<Optional<Long>> getTableVersion(
      String share, String schema, String table, String startingTimestamp);

  CompletionStage<ContentAndToken<List<Share>>> listShares(
      Optional<ContentAndToken.Token> nextPageToken, Optional<Integer> maxResults);

  CompletionStage<Optional<ContentAndToken<List<Schema>>>> listSchemas(
      String share, Optional<ContentAndToken.Token> nextPageToken, Optional<Integer> maxResults);

  CompletionStage<Optional<ContentAndToken<List<Table>>>> listTables(
      String share,
      String schema,
      Optional<ContentAndToken.Token> nextPageToken,
      Optional<Integer> maxResults);

  CompletionStage<Optional<ContentAndToken<List<Table>>>> listTablesOfShare(
      String share, Optional<ContentAndToken.Token> token, Optional<Integer> maxResults);
}
