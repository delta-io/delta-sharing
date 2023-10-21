package io.whitefox.core.storage;

import io.whitefox.annotations.SkipCoverageGenerated;
import io.whitefox.core.AwsCredentials;
import java.util.Objects;

public interface StorageProperties {
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
  }
}
