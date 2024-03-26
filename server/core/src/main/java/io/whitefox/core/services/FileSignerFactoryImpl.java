package io.whitefox.core.services;

import io.whitefox.core.AwsCredentials;
import io.whitefox.core.Storage;
import io.whitefox.core.StorageProperties;
import io.whitefox.core.TableFileIdMd5HashFunction;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class FileSignerFactoryImpl implements FileSignerFactory {

  private final S3ClientFactory s3ClientFactory;

  public FileSignerFactoryImpl(S3ClientFactory s3ClientFactory) {
    this.s3ClientFactory = s3ClientFactory;
  }

  @Override
  public FileSigner newFileSigner(Storage storage) {
    switch (storage.type()) {
      case S3:
        AwsCredentials.SimpleAwsCredentials credentials = (AwsCredentials.SimpleAwsCredentials)
            ((StorageProperties.S3Properties) storage.properties()).credentials();
        return new S3FileSigner(
            s3ClientFactory.newS3Presigner(credentials), new TableFileIdMd5HashFunction());
      case LOCAL:
        return new NoOpSigner();
      default:
        throw new RuntimeException(
            String.format("unrecognized storage type: [%s]", storage.type().value));
    }
  }
}
