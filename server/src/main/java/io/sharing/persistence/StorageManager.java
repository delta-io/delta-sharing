package io.sharing.persistence;

import io.delta.sharing.api.server.model.Share;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface StorageManager {
  CompletionStage<Optional<Share>> getShare(String share);

  CompletionStage<ResultAndTotalSize<List<Share>>> getShares(int offset, int maxResultSize);
}
