package io.sharing.persistence;

import io.delta.sharing.api.server.model.Share;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@ApplicationScoped
public class StorageManager {
  private final ConcurrentMap<String, Share> shares;

  @Inject
  public StorageManager() {
    this.shares = new ConcurrentHashMap<>();
  }

  public StorageManager(ConcurrentMap<String, Share> shares) {
    this.shares = shares;
  }

  public Optional<Share> getShare(String share) {
    return Optional.ofNullable(shares.get(share));
  }
}
