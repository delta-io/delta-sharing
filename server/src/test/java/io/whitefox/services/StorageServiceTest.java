package io.whitefox.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.whitefox.core.AwsCredentials;
import io.whitefox.core.Principal;
import io.whitefox.core.services.StorageService;
import io.whitefox.core.storage.CreateStorage;
import io.whitefox.core.storage.StorageProperties;
import io.whitefox.core.storage.StorageType;
import io.whitefox.persistence.memory.InMemoryStorageManager;
import java.time.Clock;
import org.junit.jupiter.api.Test;

public class StorageServiceTest {
  StorageService service = new StorageService(new InMemoryStorageManager(), Clock.systemUTC());

  private final CreateStorage createStorage = new CreateStorage(
      "s3_storage_prod",
      null,
      StorageType.S3,
      new Principal("Mr. Fox"),
      "s3://bucket/storage",
      false,
      new StorageProperties.S3Properties(
          new AwsCredentials.SimpleAwsCredentials("accessKey", "secretKey", "eu-east-1")));

  @Test
  public void createStorage() {
    var storage = service.createStorage(createStorage);
    assertEquals(storage.name(), createStorage.name());
    assertEquals(storage.owner().name(), createStorage.currentUser().name());
    assertEquals(storage.uri(), createStorage.uri());
    assertEquals(storage.createdBy().name(), createStorage.currentUser().name());
    assertEquals(storage.type(), createStorage.type());
    assertTrue(storage.validatedAt().isPresent());
  }

  @Test
  public void getNonExistingStorage() {
    var storage = service.getStorage("not exists");
    assertTrue(storage.isEmpty());
  }
}
