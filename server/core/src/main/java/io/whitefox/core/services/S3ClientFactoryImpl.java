package io.whitefox.core.services;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import io.whitefox.core.AwsCredentials;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class S3ClientFactoryImpl implements S3ClientFactory {

  public AmazonS3 newS3Client(AwsCredentials.SimpleAwsCredentials awsCredentials) {
    return AmazonS3Client.builder()
        .withRegion(Regions.fromName(awsCredentials.region()))
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
            awsCredentials.awsAccessKeyId(), awsCredentials.awsSecretAccessKey())))
        .build();
  }
}
