package io.whitefox.api.deltasharing.server;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.whitefox.api.deltasharing.encoders.DeltaPageTokenEncoder;

@QuarkusIntegrationTest
public class DeltaSharesApiImplIT extends DeltaSharesApiImplTest {

  public DeltaSharesApiImplIT() {
    super(new DeltaPageTokenEncoder());
  }
}
