package io.whitefox.core.services;

import io.whitefox.core.AwsCredentials;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

public interface S3ClientFactory {
  S3Client newS3Client(AwsCredentials.SimpleAwsCredentials awsCredentials);

  S3Presigner newS3Presigner(AwsCredentials.SimpleAwsCredentials awsCredentials);
}
