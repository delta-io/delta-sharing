package io.sharing.persistence;

import io.delta.sharing.api.server.model.Share;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@ApplicationScoped
public class InMemoryStorageManager implements StorageManager {
  private final ConcurrentMap<String, Share> shares;

  @Inject
  public InMemoryStorageManager() {
    this.shares = new ConcurrentHashMap<>();
  }

  public InMemoryStorageManager(ConcurrentMap<String, Share> shares) {
    this.shares = shares;
  }

  @Override
  public CompletionStage<Optional<Share>> getShare(String share) {
    return CompletableFuture.completedFuture(Optional.ofNullable(shares.get(share)));
  }
}
