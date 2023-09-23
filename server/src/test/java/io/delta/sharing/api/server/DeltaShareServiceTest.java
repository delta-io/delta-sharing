package io.delta.sharing.api.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.sharing.api.server.model.Share;
import io.quarkus.test.junit.QuarkusTest;
import io.sharing.persistence.InMemoryStorageManager;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class DeltaShareServiceTest {

  @Test
  public void getUnknownShare() throws ExecutionException, InterruptedException {
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(new InMemoryStorageManager());
    Optional<Share> unknown = deltaSharesService.getShare("unknown").toCompletableFuture().get();
    assertEquals(Optional.empty(), unknown);
  }

  @Test
  public void getShare() throws ExecutionException, InterruptedException {
    ConcurrentMap<String, Share> shares = new ConcurrentHashMap<>();
    shares.put("key", new Share().id("key").name("name"));
    DeltaSharesService deltaSharesService =
        new DeltaSharesServiceImpl(new InMemoryStorageManager(shares));
    Optional<Share> share = deltaSharesService.getShare("key").toCompletableFuture().get();
    assertTrue(share.isPresent());
    assertEquals("name", share.get().getName());
    assertEquals("key", share.get().getId());
  }
}
