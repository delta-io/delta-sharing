package io.delta.sharing.api.server;

import io.delta.sharing.api.ContentAndToken;
import io.delta.sharing.api.server.model.Share;
import io.delta.sharing.encoders.DeltaPageTokenEncoder;
import io.sharing.persistence.StorageManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class DeltaSharesServiceImpl implements DeltaSharesService {

  private final StorageManager storageManager;
  private final Integer defaultMaxResults;
  private final DeltaPageTokenEncoder encoder;

  @Inject
  public DeltaSharesServiceImpl(
      StorageManager storageManager,
      @ConfigProperty(name = "io.delta.sharing.api.server.defaultMaxResults")
          Integer defaultMaxResults,
      DeltaPageTokenEncoder encoder) {
    this.storageManager = storageManager;
    this.defaultMaxResults = defaultMaxResults;
    this.encoder = encoder;
  }

  @Override
  public CompletionStage<Optional<Share>> getShare(String share) {
    return storageManager.getShare(share);
  }

  @Override
  public CompletionStage<ContentAndToken<List<Share>>> listShares(
      Optional<ContentAndToken.Token> nextPageToken, Optional<Integer> maxResults) {
    Integer finalMaxResults = maxResults.orElse(defaultMaxResults);
    Integer start = nextPageToken
        .map(s -> Integer.valueOf(encoder.decodePageToken(s.value)))
        .orElse(0);
    var resAndSize = storageManager.getShares(start, finalMaxResults);
    int end = start + finalMaxResults;

    return resAndSize.thenApplyAsync(pageContent -> {
      Optional<String> optionalToken =
          end < pageContent.size ? Optional.of(Integer.toString(end)) : Optional.empty();
      return optionalToken
          .map(encoder::encodePageToken)
          .map(t -> ContentAndToken.of(pageContent.result, t))
          .orElse(ContentAndToken.withoutToken(pageContent.result));
    });
  }
}
