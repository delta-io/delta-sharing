package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Objects;

public interface MetastoreProperties {

  final class GlueMetastoreProperties implements MetastoreProperties {
    private final String catalogId;
    private final AwsCredentials credentials;

    public GlueMetastoreProperties(
        String catalogId, AwsCredentials credentials, MetastoreType type) {
      if (type != MetastoreType.GLUE) {
        throw new IllegalArgumentException(String.format(
            "GlueMetastore properties are not compatible with metastore of type %o", type));
      }
      this.catalogId = catalogId;
      this.credentials = credentials;
    }

    public String catalogId() {
      return catalogId;
    }

    public AwsCredentials credentials() {
      return credentials;
    }

    @Override
    @SkipCoverageGenerated
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (obj == null || obj.getClass() != this.getClass()) return false;
      var that = (GlueMetastoreProperties) obj;
      return Objects.equals(this.catalogId, that.catalogId)
          && Objects.equals(this.credentials, that.credentials);
    }

    @Override
    @SkipCoverageGenerated
    public int hashCode() {
      return Objects.hash(catalogId, credentials);
    }

    @Override
    @SkipCoverageGenerated
    public String toString() {
      return "GlueMetastoreProperties[" + "catalogId="
          + catalogId + ", " + "credentials="
          + credentials + ']';
    }
  }
}
