package io.whitefox.core.services;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import io.whitefox.core.TableFile;
import io.whitefox.core.TableFileIdHashFunction;
import io.whitefox.core.TableFileToBeSigned;
import java.net.URI;
import java.net.URL;
import java.util.Date;
import java.util.Optional;

public class S3FileSigner implements FileSigner {

  private final AmazonS3 s3Client;
  private final TableFileIdHashFunction tableFileIdHashFunction;

  public S3FileSigner(AmazonS3 s3Client, TableFileIdHashFunction tableFileIdHashFunction) {
    this.s3Client = s3Client;
    this.tableFileIdHashFunction = tableFileIdHashFunction;
  }

  @Override
  public TableFile sign(TableFileToBeSigned s) {
    URI absPath = URI.create(s.url());
    String bucketName = absPath.getHost();
    String keyName = stripPrefix(absPath.getPath(), "/");
    Date expirationDate = new Date(System.currentTimeMillis() + SECONDS.toMillis(3600));
    URL presignedUrl = s3Client.generatePresignedUrl(
        buildPresignedUrlRequest(bucketName, keyName, expirationDate));
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

  private GeneratePresignedUrlRequest buildPresignedUrlRequest(
      String bucketName, String keyName, Date expirationDate) {
    return new GeneratePresignedUrlRequest(bucketName, keyName)
        .withMethod(HttpMethod.GET)
        .withExpiration(expirationDate);
  }
}
