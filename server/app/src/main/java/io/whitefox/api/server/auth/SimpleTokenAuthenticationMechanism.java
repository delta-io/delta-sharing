package io.whitefox.api.server.auth;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.IdentityProviderManager;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.AnonymousAuthenticationRequest;
import io.quarkus.security.identity.request.AuthenticationRequest;
import io.quarkus.security.runtime.QuarkusPrincipal;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.quarkus.vertx.http.runtime.security.ChallengeData;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticationMechanism;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Inject;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class SimpleTokenAuthenticationMechanism implements HttpAuthenticationMechanism {

  private static final String AUTHORIZATION_HEADER = "Authorization";
  private static final QuarkusPrincipal principal = new QuarkusPrincipal("Mr. WhiteFox");

  String token;

  @Inject
  IdentityProviderManager identityProvider;

  public SimpleTokenAuthenticationMechanism(String token) {
    this.token = token;
  }

  private Uni<SecurityIdentity> anonymous() {
    return identityProvider.authenticate(AnonymousAuthenticationRequest.INSTANCE);
  }

  @Override
  public Uni<SecurityIdentity> authenticate(
      RoutingContext context, IdentityProviderManager identityProviderManager) {
    if (context.normalizedPath().startsWith("/q/")) return anonymous();
    var optionalHeader = Optional.ofNullable(context.request().headers().get(AUTHORIZATION_HEADER));
    QuarkusSecurityIdentity identity =
        new QuarkusSecurityIdentity.Builder().setPrincipal(principal).build();
    AuthenticationFailedException missingOrUnrecognizedCredentials =
        new AuthenticationFailedException("Missing or unrecognized credentials");
    AuthenticationFailedException missingToken = new AuthenticationFailedException(
        "Simple authentication enabled, but token is missing in the request");
    if (optionalHeader.isEmpty()) throw missingToken;
    else {
      if (Objects.equals("Bearer " + token, optionalHeader.get())) {
        return Uni.createFrom().item(identity);
      } else {
        throw missingOrUnrecognizedCredentials;
      }
    }
  }

  // Not really needed for this mechanism, does not get called;
  // The response to the user is handled through a custom attemptAuthentication from
  // WhitefoxHttpAuthenticator
  @Override
  public Uni<ChallengeData> getChallenge(RoutingContext context) {
    var challenge = "token -> " + token;
    ChallengeData result = new ChallengeData(
        HttpResponseStatus.UNAUTHORIZED.code(), HttpHeaderNames.WWW_AUTHENTICATE, challenge);
    return Uni.createFrom().item(result);
  }

  // no need for this mechanism, for others
  @Override
  public Set<Class<? extends AuthenticationRequest>> getCredentialTypes() {
    return null;
  }
}
