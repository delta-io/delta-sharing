package io.delta.sharing.api.server;

import io.delta.sharing.api.server.model.Share;
import io.sharing.persistence.StorageManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;

@ApplicationScoped
public class DeltaSharesService {

  private final StorageManager storageManager;

  @Inject
  public DeltaSharesService(StorageManager storageManager) {
    this.storageManager = storageManager;
  }

  public Optional<Share> getShare(String share) {
    return storageManager.getShare(share).map(s -> new Share().name(s.getName()));
  }
}
