package io.whitefox.api.server;

import io.whitefox.api.model.v1.generated.ProviderInput;
import io.whitefox.api.server.v1.generated.ProviderV1Api;
import io.whitefox.core.services.ProviderService;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;

public class ProviderV1ApiImpl implements ProviderV1Api, ApiUtils {

  private final ProviderService providerService;

  @Inject
  public ProviderV1ApiImpl(ProviderService providerService) {
    this.providerService = providerService;
  }

  @Override
  public Response addProvider(ProviderInput providerInput) {
    return wrapExceptions(
        () -> {
          var provider = providerService.createProvider(
              WhitefoxMappers.api2CreateProvider(providerInput, getRequestPrincipal()));
          return Response.ok(WhitefoxMappers.provider2Api(provider)).build();
        },
        exceptionToResponse);
  }

  @Override
  public Response deleteProvider(String name, String force) {
    return Response.status(501).build();
  }

  @Override
  public Response getProvider(String name) {
    return wrapExceptions(
        () -> optionalToNotFound(
            providerService.getProvider(name),
            provider -> Response.ok(WhitefoxMappers.provider2Api(provider)).build()),
        exceptionToResponse);
  }

  @Override
  public Response listProviders() {
    return Response.status(501).build();
  }

  @Override
  public Response updateProvider(String name, ProviderInput providerInput) {
    return Response.status(501).build();
  }
}
