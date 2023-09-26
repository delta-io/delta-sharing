package io.whitefox.api.server;

import io.whitefox.api.model.CreateStorage;
import io.whitefox.api.model.UpdateStorage;
import jakarta.ws.rs.core.Response;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class StorageApiImpl implements StorageApi {

  @Override
  public CompletionStage<Response> createStorage(CreateStorage createStorage) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> deleteStorage(String name, String force) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> describeStorage(String name) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> listStorage() {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> updateStorage(String name, UpdateStorage updateStorage) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> validateStorage(String name) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }
}
