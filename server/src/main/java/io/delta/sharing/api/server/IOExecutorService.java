package io.delta.sharing.api.server;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.Executors;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class IOExecutorService extends DelegateExecutorService {

  // For CDI 2.0 to work
  public IOExecutorService() {
    this(32);
  }

  @Inject
  public IOExecutorService(
      @ConfigProperty(name = "delta.sharing.api.server.nThreads") int numThreads) {
    super(Executors.newWorkStealingPool(numThreads));
  }
}
