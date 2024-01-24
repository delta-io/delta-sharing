package io.whitefox;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@Singleton
public class AwsGlueTestConfig {

  public static AwsGlueTestConfig loadFromEnv() {
    return new AwsGlueTestConfig(
        Optional.ofNullable(System.getenv().get("WHITEFOX_TEST_GLUE_CATALOG_ID")));
  }

  @Inject
  public AwsGlueTestConfig(
      @ConfigProperty(name = "whitefox.provider.aws.test.glue.catalog.id")
          Optional<String> catalogId) {
    this.catalogId = catalogId;
  }

  private final Optional<String> catalogId;

  public AwsGlueTestConfig(String catalogId) {
    this.catalogId = Optional.of(catalogId);
  }

  public String catalogId() {
    return catalogId.orElseThrow(() -> new RuntimeException(
        "Missing glue catalog configuration, " + "are you providing the aws glue catalog id?"));
  }
}
