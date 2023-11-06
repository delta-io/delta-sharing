package io.whitefox.core.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.whitefox.core.AwsCredentials;
import io.whitefox.core.MetastoreProperties;
import io.whitefox.core.MetastoreType;
import io.whitefox.core.Principal;
import io.whitefox.core.actions.CreateMetastore;
import io.whitefox.persistence.memory.InMemoryStorageManager;
import java.time.Clock;
import org.junit.jupiter.api.Test;

public class MetastoreServiceTest {
  MetastoreService service = new MetastoreService(new InMemoryStorageManager(), Clock.systemUTC());

  private final CreateMetastore createMetastore = new CreateMetastore(
      "s3_storage_prod",
      null,
      MetastoreType.GLUE,
      new MetastoreProperties.GlueMetastoreProperties(
          "catalog1",
          new AwsCredentials.SimpleAwsCredentials("accessKey", "secretKey", "eu-west-1"),
          MetastoreType.GLUE),
      new Principal("Mr. Fox"),
      false);

  @Test
  public void createMetastore() {
    var metastore = service.createMetastore(createMetastore);
    assertEquals(metastore.name(), createMetastore.name());
    assertEquals(metastore.owner().name(), createMetastore.currentUser().name());
    assertEquals(metastore.createdBy().name(), createMetastore.currentUser().name());
    assertEquals(metastore.type(), createMetastore.type());
    assertTrue(metastore.validatedAt().isPresent());
  }

  @Test
  public void getNonExistingStorage() {
    var metastore = service.getMetastore("not_exists");
    assertTrue(metastore.isEmpty());
  }
}
