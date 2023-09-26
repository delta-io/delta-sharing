package io.whitefox.api.server;

import io.whitefox.api.model.CreateTableInput;
import io.whitefox.api.model.PatchTableInput;
import io.whitefox.api.model.ProviderInput;
import jakarta.ws.rs.core.Response;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class ProvidersApiImpl implements ProvidersApi {
  @Override
  public CompletionStage<Response> addProvider(ProviderInput providerInput) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> createTableInProvider(
      String providerName, CreateTableInput createTableInput) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> deleteProvider(String name, String force) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> deleteTableInProvider(
      String providerName, String tableName, String force) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> describeTableInProvider(String providerName, String tableName) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> getProvider(String name) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> listProviders() {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> listTablesInProvider(String providerName) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> patchTableInProvider(
      String providerName, String tableName, PatchTableInput patchTableInput) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> updateProvider(String name, ProviderInput providerInput) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> validateTable(String providerName, String tableName) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }
}
