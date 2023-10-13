package io.whitefox.api.server;

import io.whitefox.api.deltasharing.Mappers;
import io.whitefox.api.model.v1.generated.UpdateMetastore;
import io.whitefox.api.server.v1.generated.MetastoreV1Api;
import io.whitefox.core.Principal;
import io.whitefox.core.services.MetastoreService;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;

public class MetastoreV1ApiImpl implements MetastoreV1Api, ApiUtils {

  private final MetastoreService metastoreService;

  @Inject
  public MetastoreV1ApiImpl(MetastoreService metastoreService) {
    this.metastoreService = metastoreService;
  }

  @Override
  public Response createMetastore(
      io.whitefox.api.model.v1.generated.CreateMetastore createMetastore) {
    return wrapExceptions(
        () -> Response.status(Response.Status.CREATED)
            .entity(Mappers.metastore2api(metastoreService.createStorageManager(
                Mappers.api2createMetastore(createMetastore, getRequestPrincipal()))))
            .build(),
        exceptionToResponse);
  }

  private Principal getRequestPrincipal() {
    return new Principal("Mr. Fox");
  }

  @Override
  public Response deleteMetastore(String name, String force) {
    Response res = Response.ok().build();
    return res;
  }

  @Override
  public Response describeMetastore(String name) {
    return wrapExceptions(
        () -> optionalToNotFound(
            metastoreService.getMetastore(name),
            metastore -> Response.ok(Mappers.metastore2api(metastore)).build()),
        exceptionToResponse);
  }

  @Override
  public Response listMetastores() {
    Response res = Response.ok().build();
    return res;
  }

  @Override
  public Response updateMetastore(String name, UpdateMetastore updateMetastore) {
    Response res = Response.ok().build();
    return res;
  }

  @Override
  public Response validateMetastore(String name) {
    Response res = Response.ok().build();
    return res;
  }
}
