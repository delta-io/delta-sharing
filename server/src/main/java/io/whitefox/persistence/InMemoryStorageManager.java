package io.whitefox.persistence;

import io.whitefox.api.deltasharing.encoders.InvalidPageTokenException;
import io.whitefox.api.deltasharing.model.Schema;
import io.whitefox.api.deltasharing.model.Share;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@ApplicationScoped
public class InMemoryStorageManager implements StorageManager {
  private final ConcurrentMap<String, Share> shares;
  private final ConcurrentMap<String, List<Schema>> schemas;

  @Inject
  public InMemoryStorageManager() {
    this.shares = new ConcurrentHashMap<>();
    this.schemas = new ConcurrentHashMap<>();
  }

  public InMemoryStorageManager(Map<String, Share> shares, Map<String, List<Schema>> schemas) {
    if (!shares.keySet().equals(schemas.keySet())) {
      throw new IllegalArgumentException("shares and schemas key should match!");
    }
    this.shares = new ConcurrentHashMap<>(shares);
    this.schemas = new ConcurrentHashMap<>(schemas);
  }

  @Override
  public CompletionStage<Optional<Share>> getShare(String share) {
    return CompletableFuture.completedFuture(Optional.ofNullable(shares.get(share)));
  }

  @Override
  public CompletionStage<ResultAndTotalSize<List<Share>>> getShares(int offset, int maxResultSize) {
    var totalSize = shares.size();
    if (offset > totalSize) {
      return CompletableFuture.failedFuture(new InvalidPageTokenException(
          String.format("Invalid Next Page Token: token %s is larger than totalSize", offset)));
    } else {
      return CompletableFuture.completedFuture(new ResultAndTotalSize<>(
          shares.values().stream().skip(offset).limit(maxResultSize).collect(Collectors.toList()),
          totalSize));
    }
  }

  @Override
  public CompletionStage<Optional<ResultAndTotalSize<List<Schema>>>> listSchemas(
      String share, int offset, int maxResultSize) {
    var schemaList = schemas.get(share);
    if (schemaList == null) {
      return CompletableFuture.completedFuture(Optional.empty());
    } else {
      var totalSize = schemaList.size();
      if (offset > totalSize) {
        return CompletableFuture.failedFuture(new InvalidPageTokenException(
            String.format("Invalid Next Page Token: token %s is larger than totalSize", offset)));
      } else {
        return CompletableFuture.completedFuture(Optional.of(new ResultAndTotalSize<>(
            schemaList.stream().skip(offset).limit(maxResultSize).collect(Collectors.toList()),
            totalSize)));
      }
    }
  }
}
