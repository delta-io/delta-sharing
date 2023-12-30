package io.whitefox.api.server.auth;

import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.IdentityProvider;
import io.quarkus.security.identity.IdentityProviderManager;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.AnonymousAuthenticationRequest;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticationMechanism;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticator;
import io.quarkus.vertx.http.runtime.security.PathMatchingHttpSecurityPolicy;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.RoutingContext;
import jakarta.annotation.Priority;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * A custom {@link HttpAuthenticator}. This authenticator that performs the following main duties:
 *
 * <ul>
 *   <li>Authenticates requests using a token provided in the application.properties when authentication is enabled.
 *   <li>Completely disallows unauthenticated requests when authentication is enabled.
 * </ul>
 */
@Alternative // @Alternative + @Priority ensure the original HttpAuthenticator bean is not used
@Priority(1)
@Singleton
public class WhitefoxHttpAuthenticator extends HttpAuthenticator {

  private final IdentityProviderManager identityProvider;
  private final boolean authEnabled;
  private final WhitefoxAuthenticationConfig config;

  @Inject
  public WhitefoxHttpAuthenticator(
      WhitefoxAuthenticationConfig config,
      IdentityProviderManager identityProviderManager,
      Instance<PathMatchingHttpSecurityPolicy> pathMatchingPolicy,
      Instance<HttpAuthenticationMechanism> httpAuthenticationMechanism,
      Instance<IdentityProvider<?>> providers) {
    super(identityProviderManager, pathMatchingPolicy, httpAuthenticationMechanism, providers);
    this.identityProvider = identityProviderManager;
    this.config = config;
    authEnabled = config.enabled();
  }

  private HttpAuthenticationMechanism selectAuthenticationMechanism(
      WhitefoxAuthenticationConfig config, RoutingContext context) {
    if (config.bearerToken().isPresent()) {
      return new SimpleTokenAuthenticationMechanism(config.bearerToken().get());
    } else {
      throw new AuthenticationFailedException(
          "Other auth mechanisms not supported right now! Please add your token to application.properties");
    }
  }

  @Override
  public Uni<SecurityIdentity> attemptAuthentication(RoutingContext context) {
    if (!authEnabled) {
      return anonymous();
    }
    // quarkus dev paths
    else if (context.normalizedPath().startsWith("/q/")) {
      return anonymous();
    } else {
      return selectAuthenticationMechanism(config, context).authenticate(context, identityProvider);
    }
  }

  private Uni<SecurityIdentity> anonymous() {
    return identityProvider.authenticate(AnonymousAuthenticationRequest.INSTANCE);
  }
}
