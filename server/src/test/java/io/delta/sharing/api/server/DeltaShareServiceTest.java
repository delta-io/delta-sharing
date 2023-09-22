package io.delta.sharing.api.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.sharing.api.server.model.Share;
import io.quarkus.test.junit.QuarkusTest;
import io.sharing.persistence.StorageManager;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;

@QuarkusTest
public class DeltaShareServiceTest {

  @Test
  public void getUnknownShare() {
    DeltaSharesService deltaSharesService = new DeltaSharesService(new StorageManager());
    Optional<Share> unknown = deltaSharesService.getShare("unknown");
    assertEquals(Optional.empty(), unknown);
  }

  @Test
  public void getShare() {
    ConcurrentMap<String, Share> shares = new ConcurrentHashMap<>();
    shares.put("key", new Share().id("key").name("name"));
    DeltaSharesService deltaSharesService = new DeltaSharesService(new StorageManager(shares));
    Optional<Share> share = deltaSharesService.getShare("key");
    assertTrue(share.isPresent());
    assertEquals("name", share.get().getName());
    assertTrue(StringUtils.isBlank(share.get().getId()));
  }
}
