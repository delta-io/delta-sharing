package io.whitefox.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.whitefox.core.*;
import io.whitefox.core.services.MetastoreService;
import io.whitefox.core.services.ProviderService;
import io.whitefox.core.services.StorageService;
import io.whitefox.core.services.exceptions.MetastoreNotFound;
import io.whitefox.core.services.exceptions.StorageNotFound;
import io.whitefox.core.storage.Storage;
import io.whitefox.core.storage.StorageType;
import io.whitefox.persistence.StorageManager;
import io.whitefox.persistence.memory.InMemoryStorageManager;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProviderServiceTest {

  Clock clock = Clock.fixed(Instant.ofEpochMilli(7), ZoneOffset.UTC);

  ProviderService service(StorageManager storageManager) {

    return new ProviderService(
        storageManager,
        new MetastoreService(storageManager, clock),
        new StorageService(storageManager, clock),
        clock);
  }

  Metastore metastore1 = new Metastore(
      "metastore1",
      Optional.empty(),
      new Principal("Mr. Fox"),
      MetastoreType.GLUE,
      new MetastoreProperties.GlueMetastoreProperties(
          "", new AwsCredentials.SimpleAwsCredentials("", "", ""), MetastoreType.GLUE),
      Optional.of(7L),
      7L,
      new Principal("Mr. Fox"),
      7L,
      new Principal("Mr. Fox"));

  Storage storage1 = new Storage(
      "storage1",
      Optional.empty(),
      new Principal("Mr. Fox"),
      StorageType.S3,
      Optional.of(7L),
      "s3://bucket/storage",
      7L,
      new Principal("Mr. Fox"),
      7L,
      new Principal("Mr. Fox"));

  @Test
  public void failToCreateProviderNoMetastore() {
    var storageManager = new InMemoryStorageManager();
    var target = service(storageManager);
    Assertions.assertThrows(
        MetastoreNotFound.class,
        () -> target.createProvider(new io.whitefox.core.actions.CreateProvider(
            "provider1",
            "storage1",
            Optional.of("metastore1"),
            new io.whitefox.core.Principal("Mr. Fox"))));
  }

  @Test
  public void failToCreateProviderNoStorage() {
    var storageManager = new InMemoryStorageManager();
    var target = service(storageManager);
    storageManager.createMetastore(metastore1);
    Assertions.assertThrows(
        StorageNotFound.class,
        () -> target.createProvider(new io.whitefox.core.actions.CreateProvider(
            "provider1",
            "storage1",
            Optional.of("metastore1"),
            new io.whitefox.core.Principal("Mr. Fox"))));
  }

  @Test
  public void createProvider() {
    var storageManager = new InMemoryStorageManager();
    var target = service(storageManager);
    storageManager.createMetastore(metastore1);
    storageManager.createStorage(storage1);
    var res = target.createProvider(new io.whitefox.core.actions.CreateProvider(
        "provider1",
        "storage1",
        Optional.of("metastore1"),
        new io.whitefox.core.Principal("Mr. Fox")));
    assertEquals(res, target.getProvider("provider1").get());
    assertEquals(
        res,
        new Provider(
            "provider1",
            storage1,
            Optional.of(metastore1),
            7L,
            new Principal("Mr. Fox"),
            7L,
            new Principal("Mr. Fox"),
            new Principal("Mr. Fox")));
  }
}
