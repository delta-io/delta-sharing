package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

public interface StorageProperties {
  void validateTypeAndUri(StorageType storageType, String uri);

  public static class S3Properties implements StorageProperties {
    private final AwsCredentials credentials;

    public S3Properties(AwsCredentials credentials) {
      this.credentials = credentials;
    }

    @Override
    @SkipCoverageGenerated
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      S3Properties that = (S3Properties) o;
      return Objects.equals(credentials, that.credentials);
    }

    @Override
    @SkipCoverageGenerated
    public int hashCode() {
      return Objects.hash(credentials);
    }

    @Override
    @SkipCoverageGenerated
    public String toString() {
      return "S3Properties{" + "credentials=" + credentials + '}';
    }

    public AwsCredentials credentials() {
      return credentials;
    }

    private static final List<String> s3Prefixes = List.of("s3://", "s3a://", "s3n://");

    @Override
    public void validateTypeAndUri(StorageType storageType, String uri) {

      if (storageType != StorageType.S3) {
        throw new IllegalArgumentException(
            String.format("Invalid storage type %s for S3Properties", storageType));
      }
      if (s3Prefixes.stream().noneMatch(pre -> StringUtils.startsWith(uri, pre))) {
        throw new IllegalArgumentException(String.format(
            "s3 uri must start with any of %s but %s was provided",
            String.join(", ", s3Prefixes), uri));
      }
    }
  }

  public static class LocalProperties implements StorageProperties {
    @Override
    public void validateTypeAndUri(StorageType storageType, String uri) {
      if (storageType != StorageType.LOCAL) {
        throw new IllegalArgumentException(
            String.format("Invalid storage type %s for LocalProprties", storageType));
      }
      if (!StringUtils.startsWith(uri, "file://")) {
        throw new IllegalArgumentException(
            String.format("local uri must start with file:// but %s was provided", uri));
      }
    }

    public Configuration hadoopConf() {
      return new Configuration();
    }
  }
}
