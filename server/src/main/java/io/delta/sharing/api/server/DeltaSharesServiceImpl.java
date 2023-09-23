package io.delta.sharing.api.server;

import io.delta.sharing.api.server.model.Share;
import io.sharing.persistence.StorageManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class DeltaSharesServiceImpl implements DeltaSharesService {

  private final StorageManager storageManager;

  @Inject
  public DeltaSharesServiceImpl(StorageManager storageManager) {
    this.storageManager = storageManager;
  }

  @Override
  public CompletionStage<Optional<Share>> getShare(String share) {
    return storageManager.getShare(share);
  }
}
