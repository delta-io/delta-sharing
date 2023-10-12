package io.whitefox.api.server;

import io.whitefox.api.model.generated.CreateStorage;
import io.whitefox.api.model.generated.UpdateStorage;
import io.whitefox.api.server.generated.StorageApi;
import jakarta.ws.rs.core.Response;

public class StorageApiImpl implements StorageApi {

  @Override
  public Response createStorage(CreateStorage createStorage) {
    return Response.ok().build();
  }

  @Override
  public Response deleteStorage(String name, String force) {
    return Response.ok().build();
  }

  @Override
  public Response describeStorage(String name) {
    return Response.ok().build();
  }

  @Override
  public Response listStorage() {
    return Response.ok().build();
  }

  @Override
  public Response updateStorage(String name, UpdateStorage updateStorage) {
    return Response.ok().build();
  }

  @Override
  public Response validateStorage(String name) {
    return Response.ok().build();
  }
}
