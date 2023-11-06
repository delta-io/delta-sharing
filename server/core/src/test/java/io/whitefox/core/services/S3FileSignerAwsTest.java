package io.whitefox.core.services;

import io.whitefox.S3TestConfig;
import io.whitefox.core.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration Tests: S3 Bucket and Delta Tables.
 *
 * These integration tests serve to validate the interaction with a dedicated test S3 bucket and Delta tables.
 * As part of the test environment, we have configured both the S3 bucket and Delta tables with
 * sample data.
 * To run the integration tests you need to obtain the required AWS credentials. They are usually provided as
 * a .env file that should be never committed to the repository.
 */
@Tag("aws")
public class S3FileSignerAwsTest {

  private final FileSignerFactory fileSignerFactory =
      new FileSignerFactoryImpl(new S3ClientFactoryImpl());
  private final S3TestConfig s3TestConfig = S3TestConfig.loadFromEnv();

  @Test
  public void signS3File() {
    var mrFoxS3Storage = getMrFoxS3Storage();
    FileSigner s3FileSigner = fileSignerFactory.newFileSigner(mrFoxS3Storage);
    TableFileToBeSigned tableFileToBeSigned = new TableFileToBeSigned(
        "s3a://whitefox-s3-test-bucket/delta/samples/delta-table/part-00007-3e2f3561-fdd6-407c-a9c9-e2d1a0f24298-c000.snappy.parquet",
        0,
        0L,
        Optional.of(1L),
        "",
        Map.of());
    var signedTable = s3FileSigner.sign(tableFileToBeSigned);
    var request = buildRequest(signedTable.url());
    var response = sendRequest(request);
    Assertions.assertEquals(200, response.statusCode());
  }

  private Storage getMrFoxS3Storage() {
    return new Storage(
        "MrFoxStorage",
        Optional.empty(),
        new Principal("Mr. Fox"),
        StorageType.S3,
        Optional.empty(),
        "uri",
        0L,
        new Principal("Mr. Fox"),
        0L,
        new Principal("Mr. Fox"),
        new StorageProperties.S3Properties(new AwsCredentials.SimpleAwsCredentials(
            s3TestConfig.accessKey(), s3TestConfig.secretKey(), s3TestConfig.region())));
  }

  private HttpRequest buildRequest(String url) {
    try {
      return HttpRequest.newBuilder().uri(new URI(url)).GET().build();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private HttpResponse<String> sendRequest(HttpRequest request) {
    try {
      return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
