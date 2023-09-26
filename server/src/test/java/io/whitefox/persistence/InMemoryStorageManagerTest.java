package io.whitefox.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.whitefox.api.deltasharing.model.Schema;
import io.whitefox.api.deltasharing.model.Share;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

public class InMemoryStorageManagerTest {

  @Test
  public void getUnknownShare() throws ExecutionException, InterruptedException {
    StorageManager storageManager = new InMemoryStorageManager();
    Optional<Share> unknown =
        storageManager.getShare("unknown").toCompletableFuture().get();
    assertEquals(Optional.empty(), unknown);
  }

  @Test
  public void getShare() throws ExecutionException, InterruptedException {
    Map<String, Share> shares = Map.of("key", new Share().id("key").name("name"));
    Map<String, List<Schema>> schemas = Map.of("key", Collections.emptyList());
    StorageManager storageManager = new InMemoryStorageManager(shares, schemas);
    Optional<Share> share = storageManager.getShare("key").toCompletableFuture().get();
    assertTrue(share.isPresent());
    assertEquals("key", share.get().getId());
    assertEquals("name", share.get().getName());
  }

  @Test
  public void listSchemasOfUnknownShare() throws ExecutionException, InterruptedException {
    Map<String, Share> shares = Map.of("key", new Share().id("key").name("name"));
    Map<String, List<Schema>> schemas = Map.of("key", Collections.emptyList());
    StorageManager storageManager = new InMemoryStorageManager(shares, schemas);
    var resultSchemas =
        storageManager.listSchemas("notKey", 0, 10).toCompletableFuture().get();
    assertTrue(resultSchemas.isEmpty());
  }

  @Test
  public void listSchemasOfEmptyShare() throws ExecutionException, InterruptedException {
    Map<String, Share> shares = Map.of("key", new Share().id("key").name("name"));
    Map<String, List<Schema>> schemas = Map.of("key", Collections.emptyList());
    StorageManager storageManager = new InMemoryStorageManager(shares, schemas);
    var resultSchemas =
        storageManager.listSchemas("key", 0, 10).toCompletableFuture().get();
    assertTrue(resultSchemas.isPresent());
    assertTrue(resultSchemas.get().result.isEmpty());
    assertEquals(0, resultSchemas.get().size);
  }

  @Test
  public void listSchemas() throws ExecutionException, InterruptedException {
    Map<String, Share> shares = Map.of("key", new Share().id("key").name("name"));
    Map<String, List<Schema>> schemas =
        Map.of("key", List.of(new Schema().share("key").name("default")));
    StorageManager storageManager = new InMemoryStorageManager(shares, schemas);
    var resultSchemas =
        storageManager.listSchemas("key", 0, 10).toCompletableFuture().get();
    assertTrue(resultSchemas.isPresent());
    assertEquals(1, resultSchemas.get().size);
    assertEquals(
        new Schema().share("key").name("default"), resultSchemas.get().result.get(0));
  }
}
