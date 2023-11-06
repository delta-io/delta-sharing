package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Objects;

public interface AwsCredentials {
  final class SimpleAwsCredentials implements AwsCredentials {
    private final String awsAccessKeyId;
    private final String awsSecretAccessKey;
    private final String region;

    public SimpleAwsCredentials(String awsAccessKeyId, String awsSecretAccessKey, String region) {
      this.awsAccessKeyId = awsAccessKeyId;
      this.awsSecretAccessKey = awsSecretAccessKey;
      this.region = region;
    }

    public String awsAccessKeyId() {
      return awsAccessKeyId;
    }

    public String awsSecretAccessKey() {
      return awsSecretAccessKey;
    }

    public String region() {
      return region;
    }

    @Override
    @SkipCoverageGenerated
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (obj == null || obj.getClass() != this.getClass()) return false;
      var that = (SimpleAwsCredentials) obj;
      return Objects.equals(this.awsAccessKeyId, that.awsAccessKeyId)
          && Objects.equals(this.awsSecretAccessKey, that.awsSecretAccessKey)
          && Objects.equals(this.region, that.region);
    }

    @Override
    @SkipCoverageGenerated
    public int hashCode() {
      return Objects.hash(awsAccessKeyId, awsSecretAccessKey, region);
    }

    @Override
    @SkipCoverageGenerated
    public String toString() {
      return "SimpleAwsCredentials[" + "awsAccessKeyId="
          + awsAccessKeyId + ", " + "awsSecretAccessKey="
          + awsSecretAccessKey + ", " + "region="
          + region + ']';
    }
  }
}
