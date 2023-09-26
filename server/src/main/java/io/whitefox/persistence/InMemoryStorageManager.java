package io.whitefox.persistence;

import io.delta.sharing.api.server.model.Share;
import io.delta.sharing.encoders.InvalidPageTokenException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

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

  @Override
  public CompletionStage<ResultAndTotalSize<List<Share>>> getShares(int offset, int maxResultSize) {
    var totalSize = shares.size();
    if (offset > totalSize) {
      throw new InvalidPageTokenException(
          String.format("Invalid Next Page Token: token %s is larger than totalSize", offset));
    }
    return CompletableFuture.completedFuture(new ResultAndTotalSize<>(
        shares.values().stream().skip(offset).limit(maxResultSize).collect(Collectors.toList()),
        totalSize));
  }
}
