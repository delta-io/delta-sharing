package io.whitefox.services;

import static org.junit.jupiter.api.Assertions.*;

import io.whitefox.api.deltasharing.encoders.DeltaPageTokenEncoder;
import io.whitefox.api.deltasharing.loader.DeltaShareTableLoader;
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
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

public class DeltaShareServiceTest {
  DeltaPageTokenEncoder encoder = new DeltaPageTokenEncoder();
  DeltaShareTableLoader loader = new DeltaShareTableLoader();
  Integer defaultMaxResults = 10;

  @Test
  public void getUnknownShare() throws ExecutionException, InterruptedException {
    DeltaSharesService deltaSharesService = new DeltaSharesServiceImpl(
        new InMemoryStorageManager(), defaultMaxResults, encoder, loader);
    Optional<Share> unknown =
        deltaSharesService.getShare("unknown").toCompletableFuture().get();
    assertEquals(Optional.empty(), unknown);
  }

  @Test
  public void getShare() throws ExecutionException, InterruptedException {
    var shares = List.of(new PShare("name", "key", Collections.emptyMap()));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, defaultMaxResults, encoder, loader);
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
        new DeltaSharesServiceImpl(storageManager, defaultMaxResults, encoder, loader);
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
        new DeltaSharesServiceImpl(storageManager, defaultMaxResults, encoder, loader);
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
        new DeltaSharesServiceImpl(storageManager, 100, encoder, loader);
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
        "name", "key", Map.of("default", new PSchema("default", Collections.emptyList(), "name"))));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, encoder, loader);
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
        "name", "key", Map.of("default", new PSchema("default", Collections.emptyList(), "name"))));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, encoder, loader);
    var resultSchemas = deltaSharesService
        .listSchemas("notKey", Optional.empty(), Optional.empty())
        .toCompletableFuture()
        .get();
    assertTrue(resultSchemas.isEmpty());
  }

  @Test
  public void listTables() throws ExecutionException, InterruptedException {
    var shares = List.of(new PShare(
        "name",
        "key",
        Map.of(
            "default",
            new PSchema(
                "default",
                List.of(new PTable("table1", "location1", "default", "name")),
                "name"))));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, encoder, loader);
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

  @Test
  public void listAllTables() throws ExecutionException, InterruptedException {
    var shares = List.of(new PShare(
        "name",
        "key",
        Map.of(
            "default",
            new PSchema(
                "default", List.of(new PTable("table1", "location1", "default", "name")), "name"),
            "other",
            new PSchema(
                "other", List.of(new PTable("table2", "location2", "default", "name")), "name"))));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, encoder, loader);
    var resultSchemas = deltaSharesService
        .listTablesOfShare("name", Optional.empty(), Optional.empty())
        .toCompletableFuture()
        .get();
    assertTrue(resultSchemas.isPresent());
    assertTrue(resultSchemas.get().getToken().isEmpty());
    Matchers.containsInAnyOrder(List.of(
            new Table().name("table2").schema("other").share("name"),
            new Table().name("table1").schema("default").share("name")))
        .matches(resultSchemas.get().getContent());
  }

  @Test
  public void listAllTablesEmpty() throws ExecutionException, InterruptedException {
    var shares = List.of(
        new PShare(
            "name",
            "key",
            Map.of(
                "default",
                new PSchema(
                    "default",
                    List.of(new PTable("table1", "location1", "default", "name")),
                    "name"),
                "other",
                new PSchema(
                    "other",
                    List.of(new PTable("table2", "location2", "default", "name")),
                    "name"))),
        new PShare("name2", "key2", Map.of()));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, encoder, loader);
    var resultSchemas = deltaSharesService
        .listTablesOfShare("name2", Optional.empty(), Optional.empty())
        .toCompletableFuture()
        .get();
    assertTrue(resultSchemas.isPresent());
    assertTrue(resultSchemas.get().getToken().isEmpty());
    assertTrue(resultSchemas.get().getContent().isEmpty());
  }

  @Test
  public void listAllTablesNoShare() throws ExecutionException, InterruptedException {
    StorageManager storageManager = new InMemoryStorageManager();
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, encoder, loader);
    var resultSchemas = deltaSharesService
        .listTablesOfShare("name2", Optional.empty(), Optional.empty())
        .toCompletableFuture()
        .get();
    assertTrue(resultSchemas.isEmpty());
  }
}
