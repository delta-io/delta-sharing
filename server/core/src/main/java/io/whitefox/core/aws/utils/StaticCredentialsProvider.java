package io.whitefox.core.aws.utils;

import java.util.Map;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class StaticCredentialsProvider implements AwsCredentialsProvider {

  private final AwsCredentials credentials;

  public static AwsCredentialsProvider create(Map<String, String> properties) {
    return software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
        retrieveCredentials(properties));
  }

  private static AwsCredentials retrieveCredentials(Map<String, String> properties) {
    if (!properties.containsKey("accessKeyId")) {
      throw new IllegalArgumentException("accessKeyId not found");
    } else if (!properties.containsKey("secretAccessKey")) {
      throw new IllegalArgumentException("secretAccessKey not found");
    }
    return AwsBasicCredentials.create(
        properties.get("accessKeyId"), properties.get("secretAccessKey"));
  }

  private StaticCredentialsProvider(Map<String, String> properties) {
    this.credentials = retrieveCredentials(properties);
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return credentials;
  }
}
