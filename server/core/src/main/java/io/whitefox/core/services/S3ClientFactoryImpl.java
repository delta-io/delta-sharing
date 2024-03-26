package io.whitefox.core.services;

import io.whitefox.core.AwsCredentials;
import jakarta.enterprise.context.ApplicationScoped;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

@ApplicationScoped
public class S3ClientFactoryImpl implements S3ClientFactory {

  public S3Client newS3Client(AwsCredentials.SimpleAwsCredentials awsCredentials) {
    return S3Client.builder()
        .region(Region.of(awsCredentials.region()))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
            awsCredentials.awsAccessKeyId(), awsCredentials.awsSecretAccessKey())))
        .build();
  }

  @Override
  public S3Presigner newS3Presigner(AwsCredentials.SimpleAwsCredentials awsCredentials) {
    return S3Presigner.builder()
        .region(Region.of(awsCredentials.region()))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
            awsCredentials.awsAccessKeyId(), awsCredentials.awsSecretAccessKey())))
        .build();
  }
}
