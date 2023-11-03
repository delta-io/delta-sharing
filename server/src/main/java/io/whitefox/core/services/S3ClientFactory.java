package io.whitefox.core.services;

import com.amazonaws.services.s3.AmazonS3;
import io.whitefox.core.AwsCredentials;

public interface S3ClientFactory {
  AmazonS3 newS3Client(AwsCredentials.SimpleAwsCredentials awsCredentials);
}
