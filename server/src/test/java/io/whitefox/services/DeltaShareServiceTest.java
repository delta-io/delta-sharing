package io.whitefox.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.whitefox.api.deltasharing.encoders.DeltaPageTokenEncoder;
import io.whitefox.api.deltasharing.model.Schema;
import io.whitefox.api.deltasharing.model.Share;
import io.whitefox.api.deltasharing.model.Table;
import io.whitefox.persistence.StorageManager;
import io.whitefox.persistence.memory.InMemoryStorageManager;
import io.whitefox.persistence.memory.PSchema;
import io.whitefox.persistence.memory.PShare;
import io.whitefox.persistence.memory.PTable;
import java.util.*;
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
    var shares = List.of(new PShare("name", "key", Collections.emptyMap()));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, defaultMaxResults, encoder);
    Optional<Share> share =
        deltaSharesService.getShare("name").toCompletableFuture().get();
    assertTrue(share.isPresent());
    assertEquals("name", share.get().getName());
    assertEquals("key", share.get().getId());
  }

  @Test
  public void listShares() throws ExecutionException, InterruptedException {
    var shares = List.of(new PShare("name", "key", Collections.emptyMap()));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, defaultMaxResults, encoder);
    var sharesWithNextToken = deltaSharesService
        .listShares(Optional.empty(), Optional.of(30))
        .toCompletableFuture()
        .get();
    assertEquals(1, sharesWithNextToken.getContent().size());
    assertTrue(sharesWithNextToken.getToken().isEmpty());
  }

  @Test
  public void listSharesWithToken() throws ExecutionException, InterruptedException {
    var shares = List.of(new PShare("name", "key", Collections.emptyMap()));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, defaultMaxResults, encoder);
    var sharesWithNextToken = deltaSharesService
        .listShares(Optional.empty(), Optional.of(30))
        .toCompletableFuture()
        .get();
    assertEquals(1, sharesWithNextToken.getContent().size());
    assertTrue(sharesWithNextToken.getToken().isEmpty());
  }

  @Test
  public void listSchemasOfEmptyShare() throws ExecutionException, InterruptedException {
    var shares = List.of(new PShare("name", "key", Collections.emptyMap()));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, encoder);
    var resultSchemas = deltaSharesService
        .listSchemas("name", Optional.empty(), Optional.empty())
        .toCompletableFuture()
        .get();
    assertTrue(resultSchemas.isPresent());
    assertTrue(resultSchemas.get().getContent().isEmpty());
    assertTrue(resultSchemas.get().getToken().isEmpty());
  }

  @Test
  public void listSchemas() throws ExecutionException, InterruptedException {
    var shares = List.of(new PShare(
        "name", "key", Map.of("default", new PSchema("default", Collections.emptyList()))));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, encoder);
    var resultSchemas = deltaSharesService
        .listSchemas("name", Optional.empty(), Optional.empty())
        .toCompletableFuture()
        .get();
    assertTrue(resultSchemas.isPresent());
    assertEquals(1, resultSchemas.get().getContent().size());
    assertEquals(
        new Schema().share("name").name("default"),
        resultSchemas.get().getContent().get(0));
    assertTrue(resultSchemas.get().getToken().isEmpty());
  }

  @Test
  public void listSchemasOfUnknownShare() throws ExecutionException, InterruptedException {
    var shares = List.of(new PShare(
        "name", "key", Map.of("default", new PSchema("default", Collections.emptyList()))));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, encoder);
    var resultSchemas = deltaSharesService
        .listSchemas("notKey", Optional.empty(), Optional.empty())
        .toCompletableFuture()
        .get();
    assertTrue(resultSchemas.isEmpty());
  }

  @Test
  public void listTables() throws ExecutionException, InterruptedException {
    var shares = List.of(new PShare(
        "name", "key", Map.of("default", new PSchema("default", List.of(new PTable("table1"))))));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, encoder);
    var resultSchemas = deltaSharesService
        .listTables("name", "default", Optional.empty(), Optional.empty())
        .toCompletableFuture()
        .get();
    assertTrue(resultSchemas.isPresent());
    assertTrue(resultSchemas.get().getToken().isEmpty());
    assertEquals(1, resultSchemas.get().getContent().size());
    assertEquals(
        new Table().name("table1").schema("default").share("name"),
        resultSchemas.get().getContent().get(0));
  }
}
