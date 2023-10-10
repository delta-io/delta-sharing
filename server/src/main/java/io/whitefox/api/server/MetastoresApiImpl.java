package io.whitefox.api.server;

import io.whitefox.api.model.CreateMetastore;
import io.whitefox.api.model.UpdateMetastore;
import jakarta.ws.rs.core.Response;

public class MetastoresApiImpl implements MetastoresApi {
  @Override
  public Response createMetastore(CreateMetastore createMetastore) {
    Response res = Response.ok().build();
    return res;
  }

  @Override
  public Response deleteMetastore(String name, String force) {
    Response res = Response.ok().build();
    return res;
  }

  @Override
  public Response describeMetastore(String name) {
    Response res = Response.ok().build();
    return res;
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
