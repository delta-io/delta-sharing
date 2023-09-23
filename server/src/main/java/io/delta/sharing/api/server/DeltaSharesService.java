package io.delta.sharing.api.server;

import io.delta.sharing.api.server.model.Share;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface DeltaSharesService {
  CompletionStage<Optional<Share>> getShare(String share);
}
