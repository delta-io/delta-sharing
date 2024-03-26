package io.whitefox.core.services;

import io.whitefox.core.TableFile;
import io.whitefox.core.TableFileIdHashFunction;
import io.whitefox.core.TableFileToBeSigned;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

public class S3FileSigner implements FileSigner {

  private final S3Presigner s3Presigner;
  private final TableFileIdHashFunction tableFileIdHashFunction;

  public S3FileSigner(S3Presigner s3Presigner, TableFileIdHashFunction tableFileIdHashFunction) {
    this.s3Presigner = s3Presigner;
    this.tableFileIdHashFunction = tableFileIdHashFunction;
  }

  @Override
  public TableFile sign(TableFileToBeSigned s) {
    URI absPath = URI.create(s.url());
    String bucketName = absPath.getHost();
    String keyName = stripPrefix(absPath.getPath(), "/");
    Duration duration = Duration.ofHours(1);
    Date expirationDate = new Date(Instant.now().plus(duration).toEpochMilli());

    URL presignedUrl = buildPresignedUrl(bucketName, keyName, duration);
    return new TableFile(
        presignedUrl.toString(),
        tableFileIdHashFunction.hash(s.url()),
        s.size(),
        Optional.of(s.version()),
        s.timestamp(),
        s.partitionValues(),
        expirationDate.getTime(),
        Optional.ofNullable(s.stats()));
  }

  private String stripPrefix(String string, String prefix) {
    if (string.startsWith(prefix)) {
      return string.substring(prefix.length());
    } else return string;
  }

  private URL buildPresignedUrl(String bucketName, String keyName, Duration duration) {

    GetObjectRequest objectRequest =
        GetObjectRequest.builder().bucket(bucketName).key(keyName).build();

    GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
        .signatureDuration(duration)
        .getObjectRequest(objectRequest)
        .build();

    PresignedGetObjectRequest presignedRequest = s3Presigner.presignGetObject(presignRequest);
    return presignedRequest.url();
  }

  @Override
  public void close() throws Exception {
    s3Presigner.close();
  }
}
