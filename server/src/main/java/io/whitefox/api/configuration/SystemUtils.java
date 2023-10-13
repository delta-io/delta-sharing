package io.whitefox.api.configuration;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.Produces;
import java.time.Clock;

public class SystemUtils {
  @Produces
  @ApplicationScoped
  Clock clock() {
    return Clock.systemUTC();
  }
}
