package io.whitefox.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.sharing.api.server.model.Share;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

public class StorageManagerTest {

  @Test
  public void getUnknownShare() throws ExecutionException, InterruptedException {
    StorageManager storageManager = new InMemoryStorageManager();
    Optional<Share> unknown =
        storageManager.getShare("unknown").toCompletableFuture().get();
    assertEquals(Optional.empty(), unknown);
  }

  @Test
  public void getShare() throws ExecutionException, InterruptedException {
    ConcurrentHashMap<String, Share> shares = new ConcurrentHashMap<>();
    shares.put("key", new Share().id("id").name("name"));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    Optional<Share> share = storageManager.getShare("key").toCompletableFuture().get();
    assertTrue(share.isPresent());
    assertEquals("id", share.get().getId());
    assertEquals("name", share.get().getName());
  }
}
