package io.whitefox.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.whitefox.api.deltasharing.encoders.DeltaPageTokenEncoder;
import io.whitefox.api.deltasharing.model.Schema;
import io.whitefox.api.deltasharing.model.Share;
import io.whitefox.persistence.InMemoryStorageManager;
import io.whitefox.persistence.StorageManager;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

public class DeltaShareServiceTest {
  DeltaPageTokenEncoder encoder = new DeltaPageTokenEncoder();
  Integer defaultMaxResults = 10;

  @Test
  public void getUnknownShare() throws ExecutionException, InterruptedException {
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(new InMemoryStorageManager(), defaultMaxResults, encoder);
    Optional<Share> unknown =
        deltaSharesService.getShare("unknown").toCompletableFuture().get();
    assertEquals(Optional.empty(), unknown);
  }

  @Test
  public void getShare() throws ExecutionException, InterruptedException {
    ConcurrentMap<String, Share> shares = new ConcurrentHashMap<>();
    shares.put("key", new Share().id("key").name("name"));
    Map<String, List<Schema>> schemas = Map.of("key", Collections.emptyList());
    DeltaSharesService deltaSharesService = new DeltaSharesServiceImpl(
        new InMemoryStorageManager(shares, schemas), defaultMaxResults, encoder);
    Optional<Share> share =
        deltaSharesService.getShare("key").toCompletableFuture().get();
    assertTrue(share.isPresent());
    assertEquals("name", share.get().getName());
    assertEquals("key", share.get().getId());
  }

  @Test
  public void listShares() throws ExecutionException, InterruptedException {
    ConcurrentMap<String, Share> shares = new ConcurrentHashMap<>();
    shares.put("key", new Share().id("key").name("name"));
    Map<String, List<Schema>> schemas = Map.of("key", Collections.emptyList());
    DeltaSharesService deltaSharesService = new DeltaSharesServiceImpl(
        new InMemoryStorageManager(shares, schemas), defaultMaxResults, encoder);
    var sharesWithNextToken = deltaSharesService
        .listShares(Optional.empty(), Optional.of(30))
        .toCompletableFuture()
        .get();
    assertEquals(1, sharesWithNextToken.getContent().size());
    assertTrue(sharesWithNextToken.getToken().isEmpty());
  }

  @Test
  public void listSharesWithToken() throws ExecutionException, InterruptedException {
    ConcurrentMap<String, Share> shares = new ConcurrentHashMap<>();
    shares.put("key", new Share().id("key").name("name"));
    Map<String, List<Schema>> schemas = Map.of("key", Collections.emptyList());
    DeltaSharesService deltaSharesService = new DeltaSharesServiceImpl(
        new InMemoryStorageManager(shares, schemas), defaultMaxResults, encoder);
    var sharesWithNextToken = deltaSharesService
        .listShares(Optional.empty(), Optional.of(30))
        .toCompletableFuture()
        .get();
    assertEquals(1, sharesWithNextToken.getContent().size());
    assertTrue(sharesWithNextToken.getToken().isEmpty());
  }

  @Test
  public void listSchemasOfEmptyShare() throws ExecutionException, InterruptedException {
    Map<String, Share> shares = Map.of("key", new Share().id("key").name("name"));
    Map<String, List<Schema>> schemas = Map.of("key", Collections.emptyList());
    StorageManager storageManager = new InMemoryStorageManager(shares, schemas);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, encoder);
    var resultSchemas = deltaSharesService
        .listSchemas("key", Optional.empty(), Optional.empty())
        .toCompletableFuture()
        .get();
    assertTrue(resultSchemas.isPresent());
    assertTrue(resultSchemas.get().getContent().isEmpty());
    assertTrue(resultSchemas.get().getToken().isEmpty());
  }

  @Test
  public void listSchemas() throws ExecutionException, InterruptedException {
    Map<String, Share> shares = Map.of("key", new Share().id("key").name("name"));
    Map<String, List<Schema>> schemas =
        Map.of("key", List.of(new Schema().share("key").name("default")));
    StorageManager storageManager = new InMemoryStorageManager(shares, schemas);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, encoder);
    var resultSchemas = deltaSharesService
        .listSchemas("key", Optional.empty(), Optional.empty())
        .toCompletableFuture()
        .get();
    assertTrue(resultSchemas.isPresent());
    assertEquals(1, resultSchemas.get().getContent().size());
    assertEquals(
        new Schema().share("key").name("default"),
        resultSchemas.get().getContent().get(0));
    assertTrue(resultSchemas.get().getToken().isEmpty());
  }

  @Test
  public void listSchemasOfUnknownShare() throws ExecutionException, InterruptedException {
    Map<String, Share> shares = Map.of("key", new Share().id("key").name("name"));
    Map<String, List<Schema>> schemas = Map.of("key", Collections.emptyList());
    StorageManager storageManager = new InMemoryStorageManager(shares, schemas);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, encoder);
    var resultSchemas = deltaSharesService
        .listSchemas("notKey", Optional.empty(), Optional.empty())
        .toCompletableFuture()
        .get();
    assertTrue(resultSchemas.isEmpty());
  }
}
