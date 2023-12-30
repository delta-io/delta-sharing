package io.whitefox.api.server.auth;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.util.Optional;

/** Configuration for Whitefox authentication settings. */
@ConfigMapping(prefix = "whitefox.server.authentication")
public interface WhitefoxAuthenticationConfig {

  /** Returns {@code true} if Whitefox authentication is enabled. */
  @WithName("enabled")
  @WithDefault("false")
  boolean enabled();

  /** Bearer token that should be used in requests to grant authorization. */
  @WithName("bearerToken")
  Optional<String> bearerToken();
}
