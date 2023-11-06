package io.whitefox.core.services;

import io.whitefox.DeltaTestUtils;
import io.whitefox.core.*;
import io.whitefox.persistence.StorageManager;
import io.whitefox.persistence.memory.InMemoryStorageManager;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

public class DeltaShareServiceTest {
  DeltaShareTableLoader loader = new DeltaShareTableLoader();
  Integer defaultMaxResults = 10;
  FileSignerFactory fileSignerFactory = new FileSignerFactoryImpl(new S3ClientFactoryImpl());

  private static final Principal testPrincipal = new Principal("Mr. Fox");

  private static Share createShare(String name, String key, Map<String, Schema> schemas) {
    return new Share(name, key, schemas, testPrincipal, 0L);
  }

  @Test
  public void listShares() throws ExecutionException, InterruptedException {
    var shares = List.of(createShare("name", "key", Collections.emptyMap()));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, defaultMaxResults, loader, fileSignerFactory);
    var sharesWithNextToken = deltaSharesService.listShares(Optional.empty(), Optional.of(30));
    Assertions.assertEquals(1, sharesWithNextToken.getContent().size());
    Assertions.assertTrue(sharesWithNextToken.getToken().isEmpty());
  }

  @Test
  public void listSharesWithToken() throws ExecutionException, InterruptedException {
    var shares = List.of(createShare("name", "key", Collections.emptyMap()));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, defaultMaxResults, loader, fileSignerFactory);
    var sharesWithNextToken = deltaSharesService.listShares(Optional.empty(), Optional.of(30));
    Assertions.assertEquals(1, sharesWithNextToken.getContent().size());
    Assertions.assertTrue(sharesWithNextToken.getToken().isEmpty());
  }

  @Test
  public void listSchemasOfEmptyShare() throws ExecutionException, InterruptedException {
    var shares = List.of(createShare("name", "key", Collections.emptyMap()));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, loader, fileSignerFactory);
    var resultSchemas = deltaSharesService.listSchemas("name", Optional.empty(), Optional.empty());
    Assertions.assertTrue(resultSchemas.isPresent());
    Assertions.assertTrue(resultSchemas.get().getContent().isEmpty());
    Assertions.assertTrue(resultSchemas.get().getToken().isEmpty());
  }

  @Test
  public void listSchemas() throws ExecutionException, InterruptedException {
    var shares = List.of(createShare(
        "name", "key", Map.of("default", new Schema("default", Collections.emptyList(), "name"))));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, loader, fileSignerFactory);
    var resultSchemas = deltaSharesService.listSchemas("name", Optional.empty(), Optional.empty());
    Assertions.assertTrue(resultSchemas.isPresent());
    Assertions.assertEquals(1, resultSchemas.get().getContent().size());
    Assertions.assertEquals(
        new Schema("default", Collections.emptyList(), "name"),
        resultSchemas.get().getContent().get(0));
    Assertions.assertTrue(resultSchemas.get().getToken().isEmpty());
  }

  @Test
  public void listSchemasOfUnknownShare() throws ExecutionException, InterruptedException {
    var shares = List.of(createShare(
        "name", "key", Map.of("default", new Schema("default", Collections.emptyList(), "name"))));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, loader, fileSignerFactory);
    var resultSchemas =
        deltaSharesService.listSchemas("notKey", Optional.empty(), Optional.empty());
    Assertions.assertTrue(resultSchemas.isEmpty());
  }

  @Test
  public void listTables() throws ExecutionException, InterruptedException {
    var shares = List.of(createShare(
        "name",
        "key",
        Map.of(
            "default",
            new Schema(
                "default",
                List.of(new SharedTable(
                    "table1", "default", "name", DeltaTestUtils.deltaTable("location1"))),
                "name"))));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, loader, fileSignerFactory);
    var resultSchemas =
        deltaSharesService.listTables("name", "default", Optional.empty(), Optional.empty());
    Assertions.assertTrue(resultSchemas.isPresent());
    Assertions.assertTrue(resultSchemas.get().getToken().isEmpty());
    Assertions.assertEquals(1, resultSchemas.get().getContent().size());
    Assertions.assertEquals(
        new SharedTable("table1", "default", "name", DeltaTestUtils.deltaTable("location1")),
        resultSchemas.get().getContent().get(0));
  }

  @Test
  public void listAllTables() throws ExecutionException, InterruptedException {
    var shares = List.of(createShare(
        "name",
        "key",
        Map.of(
            "default",
            new Schema(
                "default",
                List.of(new SharedTable(
                    "table1", "default", "name", DeltaTestUtils.deltaTable("location1"))),
                "name"),
            "other",
            new Schema(
                "other",
                List.of(new SharedTable(
                    "table2", "default", "name", DeltaTestUtils.deltaTable("location2"))),
                "name"))));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, loader, fileSignerFactory);
    var resultSchemas =
        deltaSharesService.listTablesOfShare("name", Optional.empty(), Optional.empty());
    Assertions.assertTrue(resultSchemas.isPresent());
    Assertions.assertTrue(resultSchemas.get().getToken().isEmpty());
    Matchers.containsInAnyOrder(List.of(
            new SharedTable("table2", "other", "name", null),
            new SharedTable("table1", "default", "name", null)))
        .matches(resultSchemas.get().getContent());
  }

  @Test
  public void listAllTablesEmpty() throws ExecutionException, InterruptedException {
    var shares = List.of(
        createShare(
            "name",
            "key",
            Map.of(
                "default",
                new Schema(
                    "default",
                    List.of(new SharedTable(
                        "table1", "default", "name", DeltaTestUtils.deltaTable("location1"))),
                    "name"),
                "other",
                new Schema(
                    "other",
                    List.of(new SharedTable(
                        "table2", "default", "name", DeltaTestUtils.deltaTable("location2"))),
                    "name"))),
        createShare("name2", "key2", Map.of()));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, loader, fileSignerFactory);
    var resultSchemas =
        deltaSharesService.listTablesOfShare("name2", Optional.empty(), Optional.empty());
    Assertions.assertTrue(resultSchemas.isPresent());
    Assertions.assertTrue(resultSchemas.get().getToken().isEmpty());
    Assertions.assertTrue(resultSchemas.get().getContent().isEmpty());
  }

  @Test
  public void listAllTablesNoShare() throws ExecutionException, InterruptedException {
    StorageManager storageManager = new InMemoryStorageManager();
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, loader, fileSignerFactory);
    var resultSchemas =
        deltaSharesService.listTablesOfShare("name2", Optional.empty(), Optional.empty());
    Assertions.assertTrue(resultSchemas.isEmpty());
  }

  @Test
  @DisabledOnOs(OS.WINDOWS)
  public void getTableMetadata() {
    var shares = List.of(createShare(
        "name",
        "key",
        Map.of(
            "default",
            new Schema(
                "default",
                List.of(new SharedTable(
                    "table1", "default", "name", DeltaTestUtils.deltaTable("delta-table"))),
                "name"))));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, loader, fileSignerFactory);
    var tableMetadata = deltaSharesService.getTableMetadata("name", "default", "table1", null);
    Assertions.assertTrue(tableMetadata.isPresent());
    Assertions.assertEquals(
        "56d48189-cdbc-44f2-9b0e-2bded4c79ed7", tableMetadata.get().id());
  }

  @Test
  public void tableMetadataNotFound() {
    var shares = List.of(createShare(
        "name",
        "key",
        Map.of(
            "default",
            new Schema(
                "default",
                List.of(new SharedTable(
                    "table1", "default", "name", DeltaTestUtils.deltaTable("location1"))),
                "name"))));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, loader, fileSignerFactory);
    var resultTable = deltaSharesService.getTableMetadata("name", "default", "tableNotFound", null);
    Assertions.assertTrue(resultTable.isEmpty());
  }
}
