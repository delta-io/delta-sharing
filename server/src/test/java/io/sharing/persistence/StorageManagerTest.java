package io.sharing.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.sharing.api.server.model.Share;
import io.quarkus.test.junit.QuarkusTest;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class StorageManagerTest {

  @Test
  public void getUnknownShare() {
    StorageManager storageManager = new StorageManager();
    Optional<Share> unknown = storageManager.getShare("unknown");
    assertEquals(Optional.empty(), unknown);
  }

  @Test
  public void getShare() {
    ConcurrentHashMap<String, Share> shares = new ConcurrentHashMap<>();
    shares.put("key", new Share().id("id").name("name"));
    StorageManager storageManager = new StorageManager(shares);
    Optional<Share> share = storageManager.getShare("key");
    assertTrue(share.isPresent());
    assertEquals("id", share.get().getId());
    assertEquals("name", share.get().getName());
  }
}
