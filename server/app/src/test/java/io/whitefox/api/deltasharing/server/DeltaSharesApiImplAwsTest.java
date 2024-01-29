package io.whitefox.api.deltasharing.server;

import static io.restassured.RestAssured.given;
import static io.whitefox.api.deltasharing.SampleTables.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.Header;
import io.whitefox.AwsGlueTestConfig;
import io.whitefox.S3TestConfig;
import io.whitefox.api.OpenApiValidatorUtils;
import io.whitefox.api.deltasharing.SampleTables;
import io.whitefox.api.deltasharing.model.FileObjectWithoutPresignedUrl;
import io.whitefox.api.deltasharing.model.v1.generated.*;
import io.whitefox.core.*;
import io.whitefox.core.Share;
import io.whitefox.persistence.StorageManager;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

/**
 * Integration Tests: S3 Bucket and Delta Tables.
 *
 * These integration tests serve to validate the interaction with a dedicated test S3 bucket and Delta tables.
 * As part of the test environment, we have configured both the S3 bucket and Delta tables with
 * sample data.
 * To run the integration tests you need to obtain the required AWS credentials. They are usually provided as
 * a .env file that should be never committed to the repository.
 */
@QuarkusTest
@Tag("aws")
public class DeltaSharesApiImplAwsTest implements OpenApiValidatorUtils {

  private static final StorageManager storageManager = SampleTables.createStorageManager();

  @BeforeAll
  public static void setup() {
    QuarkusMock.installMockForType(storageManager, StorageManager.class);
  }

  private final ObjectMapper objectMapper;

  private final S3TestConfig s3TestConfig;

  private final AwsGlueTestConfig awsGlueTestConfig;

  @Inject
  public DeltaSharesApiImplAwsTest(
      ObjectMapper objectMapper, S3TestConfig s3TestConfig, AwsGlueTestConfig awsGlueTestConfig) {
    this.objectMapper = objectMapper;
    this.s3TestConfig = s3TestConfig;
    this.awsGlueTestConfig = awsGlueTestConfig;
  }

  @BeforeEach
  public void updateStorageManagerWithS3Tables() {
    storageManager.createShare(new Share(
        "s3share",
        "key",
        Map.of(
            "s3schema",
            new io.whitefox.core.Schema(
                "s3schema",
                List.of(
                    new SharedTable("s3Table1", "s3schema", "s3share", s3DeltaTable1(s3TestConfig)),
                    new SharedTable(
                        "s3table-with-history",
                        "s3schema",
                        "s3share",
                        s3DeltaTableWithHistory1(s3TestConfig)),
                    new SharedTable(
                        "s3IcebergTable1",
                        "s3schema",
                        "s3share",
                        s3IcebergTable1(s3TestConfig, awsGlueTestConfig))),
                "s3share")),
        new Principal("Mr fox"),
        0L));
  }

  @Test
  @DisabledOnOs(OS.WINDOWS)
  public void icebergTableVersion() {
    given()
        .when()
        .filter(deltaFilter)
        .get(
            "delta-api/v1/shares/{share}/schemas/{schema}/tables/{table}/version",
            "s3share",
            "s3schema",
            "s3IcebergTable1")
        .then()
        .statusCode(200)
        .header("Delta-Table-Version", "1");
  }

  @Test
  @DisabledOnOs(OS.WINDOWS)
  public void icebergTableMetadata() throws IOException {
    var responseBodyLines = given()
        .when()
        .filter(deltaFilter)
        .get(
            "delta-api/v1/shares/{share}/schemas/{schema}/tables/{table}/metadata",
            "s3share",
            "s3schema",
            "s3IcebergTable1")
        .then()
        .statusCode(200)
        .extract()
        .asString()
        .split("\n");
    assertEquals(2, responseBodyLines.length);
    assertEquals(
        new ProtocolObject().protocol(new ProtocolObjectProtocol().minReaderVersion(1)),
        objectMapper.reader().readValue(responseBodyLines[0], ProtocolObject.class));
    assertEquals(
        new MetadataObject()
            .metaData(new MetadataObjectMetaData()
                .id("7819530050735196523")
                .name("metastore.test_glue_db.icebergtable1")
                .format(new FormatObject().provider("parquet"))
                .schemaString(
                    "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}}]}")
                .partitionColumns(List.of())
                .version(1L)
                ._configuration(Map.of("write.parquet.compression-codec", "zstd"))),
        objectMapper.reader().readValue(responseBodyLines[1], MetadataObject.class));
  }

  @DisabledOnOs(OS.WINDOWS)
  @Test
  public void queryTableCurrentVersion() throws IOException {
    var responseBodyLines = given()
        .when()
        .filter(deltaFilter)
        .body("{}")
        .header(new Header("Content-Type", "application/json"))
        .post(
            "delta-api/v1/shares/{share}/schemas/{schema}/tables/{table}/query",
            "s3share",
            "s3schema",
            "s3Table1")
        .then()
        .statusCode(200)
        .extract()
        .body()
        .asString()
        .split("\n");

    assertEquals(
        s3DeltaTable1Protocol,
        objectMapper.reader().readValue(responseBodyLines[0], ProtocolObject.class));
    assertEquals(
        s3DeltaTable1Metadata,
        objectMapper.reader().readValue(responseBodyLines[1], MetadataObject.class));
    var files = Arrays.stream(responseBodyLines)
        .skip(2)
        .map(line -> {
          try {
            return objectMapper
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .reader()
                .readValue(line, FileObjectWithoutPresignedUrl.class);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toSet());
    assertEquals(7, responseBodyLines.length);
    assertEquals(s3DeltaTable1FilesWithoutPresignedUrl, files);
  }

  @DisabledOnOs(OS.WINDOWS)
  @Test
  public void queryTableByVersion() throws IOException {
    var responseBodyLines = given()
        .when()
        .filter(deltaFilter)
        .body("{\"version\": 0}")
        .header(new Header("Content-Type", "application/json"))
        .post(
            "delta-api/v1/shares/{share}/schemas/{schema}/tables/{table}/query",
            "s3share",
            "s3schema",
            "s3Table1")
        .then()
        .statusCode(200)
        .extract()
        .body()
        .asString()
        .split("\n");

    assertEquals(
        s3DeltaTable1Protocol,
        objectMapper.reader().readValue(responseBodyLines[0], ProtocolObject.class));
    assertEquals(
        s3DeltaTable1Metadata,
        objectMapper.reader().readValue(responseBodyLines[1], MetadataObject.class));
    var files = Arrays.stream(responseBodyLines)
        .skip(2)
        .map(line -> {
          try {
            return objectMapper
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .reader()
                .readValue(line, FileObjectWithoutPresignedUrl.class);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toSet());
    assertEquals(7, responseBodyLines.length);
    assertEquals(s3DeltaTable1FilesWithoutPresignedUrl, files);
  }
}
