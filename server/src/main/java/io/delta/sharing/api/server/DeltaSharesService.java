package io.delta.sharing.api.server;

import io.delta.sharing.api.ContentAndToken;
import io.delta.sharing.api.server.model.Share;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface DeltaSharesService {
  CompletionStage<Optional<Share>> getShare(String share);

  CompletionStage<ContentAndToken<List<Share>>> listShares(
      Optional<ContentAndToken.Token> nextPageToken, Optional<Integer> maxResults);
}
