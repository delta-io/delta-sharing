package io.whitefox.api.server;

import io.whitefox.api.model.generated.CreateTableInput;
import io.whitefox.api.model.generated.PatchTableInput;
import io.whitefox.api.model.generated.ProviderInput;
import io.whitefox.api.server.generated.ProvidersApi;
import jakarta.ws.rs.core.Response;

public class ProvidersApiImpl implements ProvidersApi {
  @Override
  public Response addProvider(ProviderInput providerInput) {
    return Response.ok().build();
  }

  @Override
  public Response createTableInProvider(String providerName, CreateTableInput createTableInput) {
    return Response.ok().build();
  }

  @Override
  public Response deleteProvider(String name, String force) {
    return Response.ok().build();
  }

  @Override
  public Response deleteTableInProvider(String providerName, String tableName, String force) {
    return Response.ok().build();
  }

  @Override
  public Response describeTableInProvider(String providerName, String tableName) {
    return Response.ok().build();
  }

  @Override
  public Response getProvider(String name) {
    return Response.ok().build();
  }

  @Override
  public Response listProviders() {
    return Response.ok().build();
  }

  @Override
  public Response listTablesInProvider(String providerName) {
    return Response.ok().build();
  }

  @Override
  public Response patchTableInProvider(
      String providerName, String tableName, PatchTableInput patchTableInput) {
    return Response.ok().build();
  }

  @Override
  public Response updateProvider(String name, ProviderInput providerInput) {
    return Response.ok().build();
  }

  @Override
  public Response validateTable(String providerName, String tableName) {
    return Response.ok().build();
  }
}
