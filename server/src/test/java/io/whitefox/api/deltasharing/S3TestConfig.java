package io.whitefox.api.deltasharing;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@Singleton
public class S3TestConfig {

  private final Optional<String> region;
  private final Optional<String> accessKey;
  private final Optional<String> secretKey;

  @Inject
  public S3TestConfig(
      @ConfigProperty(name = "whitefox.provider.aws.test.region") Optional<String> region,
      @ConfigProperty(name = "whitefox.provider.aws.test.accessKey") Optional<String> accessKey,
      @ConfigProperty(name = "whitefox.provider.aws.test.secretKey") Optional<String> secretKey) {
    this.region = region;
    this.accessKey = accessKey;
    this.secretKey = secretKey;
  }

  public S3TestConfig(String region, String accessKey, String secretKey) {
    this.region = Optional.of(region);
    this.accessKey = Optional.of(accessKey);
    this.secretKey = Optional.of(secretKey);
  }

  public String region() {
    return region.orElseThrow(() -> new RuntimeException("Missing region configuration, "
        + "are you providing the necessary environment variables containing credentials?"));
  }

  public String accessKey() {
    return accessKey.orElseThrow(() -> new RuntimeException("Missing access key configuration, "
        + "are you providing the necessary environment variables containing credentials?"));
  }

  public String secretKey() {
    return secretKey.orElseThrow(() -> new RuntimeException("Missing secret key configuration, "
        + "are you providing the necessary environment variables containing credentials?"));
  }
}
