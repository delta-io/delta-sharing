package io.whitefox.core.services;

import static org.junit.jupiter.api.Assertions.*;

import io.whitefox.core.*;
import io.whitefox.core.actions.CreateInternalTable;
import io.whitefox.core.actions.CreateProvider;
import io.whitefox.core.actions.CreateShare;
import io.whitefox.core.actions.CreateStorage;
import io.whitefox.core.services.exceptions.*;
import io.whitefox.persistence.StorageManager;
import io.whitefox.persistence.memory.InMemoryStorageManager;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

public class ShareServiceTest {

  Principal testPrincipal = new Principal("Mr. Fox");
  Clock testClock = Clock.fixed(Instant.ofEpochMilli(7), ZoneOffset.UTC);

  @Test
  void createShare() {
    var storage = new InMemoryStorageManager();
    var providerService = newProviderService(storage, testClock);
    var target = new ShareService(storage, providerService, testClock);
    var result = target.createShare(emptyCreateShare(), testPrincipal);
    assertEquals(
        new Share(
            "share1",
            "share1",
            Collections.emptyMap(),
            Optional.empty(),
            Set.of(),
            7,
            testPrincipal,
            7,
            testPrincipal,
            testPrincipal),
        result);
  }

  @Test
  void failToCreateShare() {
    var storage = new InMemoryStorageManager();
    var target = new ShareService(storage, newProviderService(storage, testClock), testClock);
    target.createShare(emptyCreateShare(), testPrincipal);
    assertThrows(
        ShareAlreadyExists.class, () -> target.createShare(emptyCreateShare(), testPrincipal));
  }

  @Test
  void addRecipientsToShare() {
    var storage = new InMemoryStorageManager();
    var target = new ShareService(storage, newProviderService(storage, testClock), testClock);
    target.createShare(emptyCreateShare(), testPrincipal);
    target.addRecipientsToShare(
        "share1", List.of("Antonio", "Marco", "Aleksandar"), Principal::new, testPrincipal);
    var result = target.getShare("share1").get();
    assertEquals(
        new Share(
            "share1",
            "share1",
            Collections.emptyMap(),
            Optional.empty(),
            Set.of(new Principal("Antonio"), new Principal("Marco"), new Principal("Aleksandar")),
            7,
            testPrincipal,
            7,
            testPrincipal,
            testPrincipal),
        result);
  }

  @Test
  void addSameRecipientTwice() {
    var storage = new InMemoryStorageManager();
    var target = new ShareService(storage, newProviderService(storage, testClock), testClock);
    target.createShare(emptyCreateShare(), testPrincipal);
    target.addRecipientsToShare(
        "share1", List.of("Antonio", "Marco", "Aleksandar"), Principal::new, testPrincipal);
    Share expected = new Share(
        "share1",
        "share1",
        Collections.emptyMap(),
        Optional.empty(),
        Set.of(new Principal("Antonio"), new Principal("Marco"), new Principal("Aleksandar")),
        7,
        testPrincipal,
        7,
        testPrincipal,
        testPrincipal);
    assertEquals(expected, target.getShare("share1").get());
    target.addRecipientsToShare("share1", List.of("Antonio"), Principal::new, testPrincipal);
    assertEquals(expected, target.getShare("share1").get());
  }

  @Test
  public void addRecipientToUnknownShare() {
    var storage = new InMemoryStorageManager();
    var target = new ShareService(storage, newProviderService(storage, testClock), testClock);
    assertThrows(
        ShareNotFound.class,
        () -> target.addRecipientsToShare(
            "share1", List.of("Antonio", "Marco", "Aleksandar"), Principal::new, testPrincipal));
  }

  @Test
  public void getUnknownShare() throws ExecutionException, InterruptedException {
    var storage = new InMemoryStorageManager();
    var target = new ShareService(storage, newProviderService(storage, testClock), testClock);
    assertEquals(Optional.empty(), target.getShare("unknown"));
  }

  @Test
  public void getShare() throws ExecutionException, InterruptedException {
    var shares = List.of(createShare("name", "key", Collections.emptyMap()));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    var target =
        new ShareService(storageManager, newProviderService(storageManager, testClock), testClock);
    var share = target.getShare("name");
    assertTrue(share.isPresent());
    assertEquals("name", share.get().name());
    assertEquals("key", share.get().id());
  }

  @Test
  public void createSchema() {
    var storage = new InMemoryStorageManager();
    var target = new ShareService(storage, newProviderService(storage, testClock), testClock);
    target.createShare(emptyCreateShare(), testPrincipal);
    var result = target.createSchema("share1", "schema1", testPrincipal);
    assertEquals(
        new Share(
            "share1",
            "share1",
            Map.of("schema1", new Schema("schema1", Collections.emptyList(), "share1")),
            Optional.empty(),
            Set.of(),
            7,
            testPrincipal,
            7,
            testPrincipal,
            testPrincipal),
        result);
  }

  @Test
  public void failToCreateSameSchema() {
    var storage = new InMemoryStorageManager();
    var target = new ShareService(storage, newProviderService(storage, testClock), testClock);
    target.createShare(emptyCreateShare(), testPrincipal);
    target.createSchema("share1", "schema1", testPrincipal);
    assertThrows(
        SchemaAlreadyExists.class, () -> target.createSchema("share1", "schema1", testPrincipal));
  }

  @Test
  public void createSecondSchema() {
    var storage = new InMemoryStorageManager();
    var target = new ShareService(storage, newProviderService(storage, testClock), testClock);
    target.createShare(emptyCreateShare(), testPrincipal);
    target.createSchema("share1", "schema1", testPrincipal);
    var result = target.createSchema("share1", "schema2", testPrincipal);
    assertEquals(
        new Share(
            "share1",
            "share1",
            Map.of(
                "schema1",
                new Schema("schema1", Collections.emptyList(), "share1"),
                "schema2",
                new Schema("schema2", Collections.emptyList(), "share1")),
            Optional.empty(),
            Set.of(),
            7,
            testPrincipal,
            7,
            testPrincipal,
            testPrincipal),
        result);
  }

  @Test
  public void addTableToSchema() {
    var storage = new InMemoryStorageManager();
    var metastoreService = new MetastoreService(storage, testClock);
    var storageService = new StorageService(storage, testClock);
    var providerService = new ProviderService(storage, metastoreService, storageService, testClock);
    var tableService = new TableService(storage, testClock, providerService);
    var target = new ShareService(storage, newProviderService(storage, testClock), testClock);
    var storageObj = storageService.createStorage(new CreateStorage(
        "storage1",
        Optional.empty(),
        StorageType.S3,
        testPrincipal,
        "file://a/b/c",
        false,
        new StorageProperties.S3Properties(new AwsCredentials.SimpleAwsCredentials("", "", ""))));
    var providerObj = providerService.createProvider(
        new CreateProvider("provider1", storageObj.name(), Optional.empty(), testPrincipal));
    var tableObj = tableService.createInternalTable(
        providerObj.name(),
        testPrincipal,
        new CreateInternalTable(
            "table1",
            Optional.empty(),
            false,
            new InternalTable.DeltaTableProperties("file://a/b/c/table")));
    target.createShare(emptyCreateShare(), testPrincipal);
    target.createSchema("share1", "schema1", testPrincipal);

    target.addTableToSchema(
        new ShareName("share1"),
        new SchemaName("schema1"),
        new SharedTableName("shared-1"),
        new ProviderName(providerObj.name()),
        new InternalTableName(tableObj.name()),
        testPrincipal);
    var sharedTables = target.getShare("share1").get().schemas().get("schema1").tables();
    assertEquals(1, sharedTables.size());
    var expected = new SharedTable("shared-1", "schema1", "share1", tableObj);
    assertEquals(expected, sharedTables.stream().findFirst().get());
    var tablesFromDeltaService = new DeltaSharesServiceImpl(
            storage,
            100,
            new DeltaShareTableLoader(),
            new FileSignerFactoryImpl(new S3ClientFactoryImpl()))
        .listTablesOfShare("share1", Optional.empty(), Optional.empty())
        .get()
        .getContent();
    assertEquals(1, tablesFromDeltaService.size());
    assertEquals(expected, tablesFromDeltaService.get(0));
  }

  @Test
  public void failToAddTableToSchema() {
    var storage = new InMemoryStorageManager();
    var metastoreService = new MetastoreService(storage, testClock);
    var storageService = new StorageService(storage, testClock);
    var providerService = new ProviderService(storage, metastoreService, storageService, testClock);
    var target = new ShareService(storage, newProviderService(storage, testClock), testClock);
    Supplier<Share> addTable = () -> target.addTableToSchema(
        new ShareName("share1"),
        new SchemaName("schema1"),
        new SharedTableName("shared-1"),
        new ProviderName("provider1"),
        new InternalTableName("table1"),
        testPrincipal);
    assertThrows(ShareNotFound.class, addTable::get);
    target.createShare(emptyCreateShare(), testPrincipal);
    assertThrows(SchemaNotFound.class, addTable::get);
    target.createSchema("share1", "schema1", testPrincipal);
    assertThrows(ProviderNotFound.class, addTable::get);
    var storageObj = storageService.createStorage(new CreateStorage(
        "storage1",
        Optional.empty(),
        StorageType.S3,
        testPrincipal,
        "file://a/b/c",
        false,
        new StorageProperties.S3Properties(new AwsCredentials.SimpleAwsCredentials("", "", ""))));
    providerService.createProvider(
        new CreateProvider("provider1", storageObj.name(), Optional.empty(), testPrincipal));
    assertThrows(TableNotFound.class, addTable::get);
  }

  @Test
  public void addSameTableTwice() {
    var storage = new InMemoryStorageManager();
    var target = new ShareService(storage, newProviderService(storage, testClock), testClock);
    target.createShare(emptyCreateShare(), testPrincipal);
    target.createSchema("share1", "schema1", testPrincipal);
    var result = target.createSchema("share1", "schema2", testPrincipal);
    assertEquals(
        new Share(
            "share1",
            "share1",
            Map.of(
                "schema1",
                new Schema("schema1", Collections.emptyList(), "share1"),
                "schema2",
                new Schema("schema2", Collections.emptyList(), "share1")),
            Optional.empty(),
            Set.of(),
            7,
            testPrincipal,
            7,
            testPrincipal,
            testPrincipal),
        result);
  }

  @Test
  public void addSameTableTwiceToSchema() {
    var storage = new InMemoryStorageManager();
    var metastoreService = new MetastoreService(storage, testClock);
    var storageService = new StorageService(storage, testClock);
    var providerService = new ProviderService(storage, metastoreService, storageService, testClock);
    var tableService = new TableService(storage, testClock, providerService);

    var tableObj = ShareServiceFixture.setupInternalTable(
        storageService,
        providerService,
        tableService,
        testPrincipal,
        "storage1",
        "provider1",
        "table1");
    var tableObj2 = tableService.createInternalTable(
        tableObj.provider().name(),
        testPrincipal,
        new CreateInternalTable(
            "table2",
            Optional.empty(),
            false,
            new InternalTable.DeltaTableProperties("file://a/b/c/table")));

    var target = new ShareService(storage, newProviderService(storage, testClock), testClock);
    target.createShare(emptyCreateShare(), testPrincipal);
    target.createSchema("share1", "schema1", testPrincipal);

    target.addTableToSchema(
        new ShareName("share1"),
        new SchemaName("schema1"),
        new SharedTableName("shared-1"),
        new ProviderName(tableObj.provider().name()),
        new InternalTableName(tableObj.name()),
        testPrincipal);
    var sharedTables = target.getShare("share1").get().schemas().get("schema1").tables();
    assertEquals(1, sharedTables.size());
    var expected = new SharedTable("shared-1", "schema1", "share1", tableObj);
    assertEquals(expected, sharedTables.stream().findFirst().get());
    target.addTableToSchema(
        new ShareName("share1"),
        new SchemaName("schema1"),
        new SharedTableName("shared-1"),
        new ProviderName(tableObj.provider().name()),
        new InternalTableName(tableObj2.name()),
        testPrincipal);
    var sharedTables2 = target.getShare("share1").get().schemas().get("schema1").tables();
    assertEquals(1, sharedTables.size());
    var expected2 = new SharedTable("shared-1", "schema1", "share1", tableObj2);
    assertEquals(expected2, sharedTables2.stream().findFirst().get());
  }

  private Share createShare(String name, String key, Map<String, Schema> schemas) {
    return new Share(name, key, schemas, testPrincipal, 0L);
  }

  private CreateShare emptyCreateShare() {
    return new CreateShare(
        "share1", Optional.empty(), Collections.emptyList(), Collections.emptyList());
  }

  private ProviderService newProviderService(StorageManager storage, Clock testClock) {
    return new ProviderService(
        storage,
        new MetastoreService(storage, testClock),
        new StorageService(storage, testClock),
        testClock);
  }
}
