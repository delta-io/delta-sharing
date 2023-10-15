package io.whitefox.api.server;

import io.whitefox.api.deltasharing.Mappers;
import io.whitefox.api.model.v1.generated.UpdateStorage;
import io.whitefox.api.server.v1.generated.StorageV1Api;
import io.whitefox.core.Principal;
import io.whitefox.core.services.StorageService;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;

public class StorageV1ApiImpl implements StorageV1Api, ApiUtils {

  private final StorageService storageService;

  @Inject
  public StorageV1ApiImpl(StorageService storageService) {
    this.storageService = storageService;
  }

  @Override
  public Response createStorage(io.whitefox.api.model.v1.generated.CreateStorage createStorage) {
    return wrapExceptions(
        () -> Response.status(Response.Status.CREATED)
            .entity(Mappers.storage2api(storageService.createStorageManager(
                Mappers.api2createStorage(createStorage, getRequestPrincipal()))))
            .build(),
        exceptionToResponse);
  }

  private Principal getRequestPrincipal() {
    return new Principal("Mr. Fox");
  }

  @Override
  public Response deleteStorage(String name, String force) {
    return Response.ok().build();
  }

  @Override
  public Response describeStorage(String name) {
    return wrapExceptions(
        () -> optionalToNotFound(
            storageService.getStorage(name),
            storage -> Response.ok(Mappers.storage2api(storage)).build()),
        exceptionToResponse);
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
