package io.whitefox.core.services;

import static org.junit.jupiter.api.Assertions.*;

import io.whitefox.core.Principal;
import io.whitefox.core.Schema;
import io.whitefox.core.Share;
import io.whitefox.core.actions.CreateShare;
import io.whitefox.core.services.exceptions.SchemaAlreadyExists;
import io.whitefox.core.services.exceptions.ShareAlreadyExists;
import io.whitefox.core.services.exceptions.ShareNotFound;
import io.whitefox.persistence.StorageManager;
import io.whitefox.persistence.memory.InMemoryStorageManager;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

class ShareServiceTest {

  Principal testPrincipal = new Principal("Mr. Fox");
  Clock testClock = Clock.fixed(Instant.ofEpochMilli(7), ZoneOffset.UTC);

  @Test
  void createShare() {
    var storage = new InMemoryStorageManager();
    var target = new ShareService(storage, testClock);
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
    var target = new ShareService(storage, testClock);
    target.createShare(emptyCreateShare(), testPrincipal);
    assertThrows(
        ShareAlreadyExists.class, () -> target.createShare(emptyCreateShare(), testPrincipal));
  }

  @Test
  void addRecipientsToShare() {
    var storage = new InMemoryStorageManager();
    var target = new ShareService(storage, testClock);
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
    var target = new ShareService(storage, testClock);
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
    var target = new ShareService(storage, testClock);
    assertThrows(
        ShareNotFound.class,
        () -> target.addRecipientsToShare(
            "share1", List.of("Antonio", "Marco", "Aleksandar"), Principal::new, testPrincipal));
  }

  @Test
  public void getUnknownShare() throws ExecutionException, InterruptedException {
    var storage = new InMemoryStorageManager();
    var target = new ShareService(storage, testClock);
    assertEquals(Optional.empty(), target.getShare("unknown"));
  }

  @Test
  public void getShare() throws ExecutionException, InterruptedException {
    var shares = List.of(createShare("name", "key", Collections.emptyMap()));
    StorageManager storageManager = new InMemoryStorageManager(shares);
    var target = new ShareService(storageManager, testClock);
    var share = target.getShare("name");
    assertTrue(share.isPresent());
    assertEquals("name", share.get().name());
    assertEquals("key", share.get().id());
  }

  @Test
  public void createSchema() {
    var storage = new InMemoryStorageManager();
    var target = new ShareService(storage, testClock);
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
    var target = new ShareService(storage, testClock);
    target.createShare(emptyCreateShare(), testPrincipal);
    target.createSchema("share1", "schema1", testPrincipal);
    assertThrows(
        SchemaAlreadyExists.class, () -> target.createSchema("share1", "schema1", testPrincipal));
  }

  @Test
  public void createSecondSchema() {
    var storage = new InMemoryStorageManager();
    var target = new ShareService(storage, testClock);
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

  private Share createShare(String name, String key, Map<String, Schema> schemas) {
    return new Share(name, key, schemas, testPrincipal, 0L);
  }

  private CreateShare emptyCreateShare() {
    return new CreateShare(
        "share1", Optional.empty(), Collections.emptyList(), Collections.emptyList());
  }
}
