package io.whitefox.core.services;

import io.whitefox.core.Principal;
import io.whitefox.core.Schema;
import io.whitefox.core.Share;
import io.whitefox.core.actions.CreateShare;
import io.whitefox.core.services.exceptions.ShareAlreadyExists;
import io.whitefox.core.services.exceptions.ShareNotFound;
import io.whitefox.persistence.StorageManager;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@ApplicationScoped
public class ShareService {

  private final StorageManager storageManager;
  private final Clock clock;

  public ShareService(StorageManager storageManager, Clock clock) {
    this.storageManager = storageManager;
    this.clock = clock;
  }

  public Share createShare(CreateShare createShare, Principal currentUser) {
    if (storageManager.getShare(createShare.name()).isPresent()) {
      throw new ShareAlreadyExists("Share already exists");
    }
    var newSchemas = createShare.schemas().stream()
        .map(schemaName -> new Schema(schemaName, Collections.emptyList(), createShare.name()))
        .collect(Collectors.toMap(Schema::name, schema -> schema));
    Share share = new Share(
        createShare.name(),
        createShare.name(), // TODO
        newSchemas,
        createShare.comment(),
        Set.of(),
        clock.millis(),
        currentUser,
        clock.millis(),
        currentUser,
        currentUser);
    return storageManager.createShare(share);
  }

  public Share addRecipientsToShare(
      String share,
      List<String> principals,
      Function<String, Principal> resolvePrincipal,
      Principal requestPrincipal) {
    var shareObj = storageManager
        .getShare(share)
        .orElseThrow(() -> new ShareNotFound("Share " + share + "not found"));
    var recipientsToAdd = principals.stream().map(resolvePrincipal).collect(Collectors.toList());
    var newShare = shareObj.addRecipients(recipientsToAdd, requestPrincipal, clock.millis());
    return storageManager.updateShare(newShare);
  }

  public Optional<Share> getShare(String share) {
    return storageManager.getShare(share);
  }
}
