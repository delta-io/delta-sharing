package io.whitefox.core.services;

import io.whitefox.DeltaTestUtils;
import io.whitefox.core.*;
import io.whitefox.core.Principal;
import io.whitefox.core.Schema;
import io.whitefox.core.Share;
import io.whitefox.core.SharedTable;
import io.whitefox.core.services.capabilities.ClientCapabilities;
import io.whitefox.core.services.exceptions.TableNotFound;
import io.whitefox.persistence.StorageManager;
import io.whitefox.persistence.memory.InMemoryStorageManager;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

public class DeltaShareServiceTest {
  TableLoaderFactory tableLoaderFactory = new TableLoaderFactoryImpl();
  Integer defaultMaxResults = 10;
  FileSignerFactory fileSignerFactory = new FileSignerFactoryImpl(new S3ClientFactoryImpl());

  private static final Principal testPrincipal = new Principal("Mr. Fox");

  private static Share createShare(String name, String key, Map<String, Schema> schemas) {
    return new Share(name, key, schemas, testPrincipal, 0L);
  }

  @Test
  public void listShares() {
    var shares = List.of(createShare("name", "key", Collections.emptyMap()));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService = new DeltaSharesServiceImpl(
        storageManager, defaultMaxResults, tableLoaderFactory, fileSignerFactory);
    var sharesWithNextToken = deltaSharesService.listShares(Optional.empty(), Optional.of(30));
    Assertions.assertEquals(1, sharesWithNextToken.getContent().size());
    Assertions.assertTrue(sharesWithNextToken.getToken().isEmpty());
  }

  @Test
  public void listSharesWithToken() {
    var shares = List.of(createShare("name", "key", Collections.emptyMap()));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService = new DeltaSharesServiceImpl(
        storageManager, defaultMaxResults, tableLoaderFactory, fileSignerFactory);
    var sharesWithNextToken = deltaSharesService.listShares(Optional.empty(), Optional.of(30));
    Assertions.assertEquals(1, sharesWithNextToken.getContent().size());
    Assertions.assertTrue(sharesWithNextToken.getToken().isEmpty());
  }

  @Test
  public void listSchemasOfEmptyShare() {
    var shares = List.of(createShare("name", "key", Collections.emptyMap()));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, tableLoaderFactory, fileSignerFactory);
    var resultSchemas = deltaSharesService.listSchemas("name", Optional.empty(), Optional.empty());
    Assertions.assertTrue(resultSchemas.isPresent());
    Assertions.assertTrue(resultSchemas.get().getContent().isEmpty());
    Assertions.assertTrue(resultSchemas.get().getToken().isEmpty());
  }

  @Test
  public void listSchemas() {
    var shares = List.of(createShare(
        "name", "key", Map.of("default", new Schema("default", Collections.emptyList(), "name"))));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, tableLoaderFactory, fileSignerFactory);
    var resultSchemas = deltaSharesService.listSchemas("name", Optional.empty(), Optional.empty());
    Assertions.assertTrue(resultSchemas.isPresent());
    Assertions.assertEquals(1, resultSchemas.get().getContent().size());
    Assertions.assertEquals(
        new Schema("default", Collections.emptyList(), "name"),
        resultSchemas.get().getContent().get(0));
    Assertions.assertTrue(resultSchemas.get().getToken().isEmpty());
  }

  @Test
  public void listSchemasOfUnknownShare() {
    var shares = List.of(createShare(
        "name", "key", Map.of("default", new Schema("default", Collections.emptyList(), "name"))));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, tableLoaderFactory, fileSignerFactory);
    var resultSchemas =
        deltaSharesService.listSchemas("notKey", Optional.empty(), Optional.empty());
    Assertions.assertTrue(resultSchemas.isEmpty());
  }

  @Test
  public void listTables() {
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
        new DeltaSharesServiceImpl(storageManager, 100, tableLoaderFactory, fileSignerFactory);
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
  public void listAllTables() {
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
        new DeltaSharesServiceImpl(storageManager, 100, tableLoaderFactory, fileSignerFactory);
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
  public void listAllTablesEmpty() {
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
        new DeltaSharesServiceImpl(storageManager, 100, tableLoaderFactory, fileSignerFactory);
    var resultSchemas =
        deltaSharesService.listTablesOfShare("name2", Optional.empty(), Optional.empty());
    Assertions.assertTrue(resultSchemas.isPresent());
    Assertions.assertTrue(resultSchemas.get().getToken().isEmpty());
    Assertions.assertTrue(resultSchemas.get().getContent().isEmpty());
  }

  @Test
  public void listAllTablesNoShare() {
    StorageManager storageManager = new InMemoryStorageManager();
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, tableLoaderFactory, fileSignerFactory);
    var resultSchemas =
        deltaSharesService.listTablesOfShare("name2", Optional.empty(), Optional.empty());
    Assertions.assertTrue(resultSchemas.isEmpty());
  }

  @Test
  @DisabledOnOs(OS.WINDOWS)
  public void getDeltaTableMetadata() {
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
        new DeltaSharesServiceImpl(storageManager, 100, tableLoaderFactory, fileSignerFactory);
    var tableMetadata = deltaSharesService.getTableMetadata(
        "name", "default", "table1", Optional.empty(), ClientCapabilities.parquet());
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
        new DeltaSharesServiceImpl(storageManager, 100, tableLoaderFactory, fileSignerFactory);
    var resultTable = deltaSharesService.getTableMetadata(
        "name", "default", "tableNotFound", Optional.empty(), ClientCapabilities.parquet());
    Assertions.assertTrue(resultTable.isEmpty());
  }

  @Test
  @DisabledOnOs(OS.WINDOWS)
  public void queryExistingTable() {
    var shares = List.of(createShare(
        "name",
        "key",
        Map.of(
            "default",
            new Schema(
                "default",
                List.of(new SharedTable(
                    "partitioned-delta-table",
                    "default",
                    "name",
                    DeltaTestUtils.deltaTable("partitioned-delta-table"))),
                "name"))));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(storageManager, 100, tableLoaderFactory, fileSignerFactory);
    var resultTable = deltaSharesService.queryTable(
        "name",
        "default",
        "partitioned-delta-table",
        new ReadTableRequest.ReadTableCurrentVersion(
            Optional.empty(), Optional.empty(), Optional.empty()),
        ClientCapabilities.parquet());
    Assertions.assertEquals(9, resultTable.files().size());
  }

  @Test
  public void queryNonExistingTable() {
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
        new DeltaSharesServiceImpl(storageManager, 100, tableLoaderFactory, fileSignerFactory);
    Assertions.assertThrows(
        TableNotFound.class,
        () -> deltaSharesService.queryTable(
            "name", "default", "tableNotFound", null, ClientCapabilities.parquet()));
  }
}
