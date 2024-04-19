package io.whitefox.api.deltasharing.server;

import static io.restassured.RestAssured.given;
import static io.whitefox.api.deltasharing.SampleTables.*;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.Header;
import io.whitefox.api.OpenApiValidatorUtils;
import io.whitefox.api.deltasharing.SampleTables;
import io.whitefox.api.deltasharing.encoders.DeltaPageTokenEncoder;
import io.whitefox.api.deltasharing.model.FileObjectWithoutPresignedUrl;
import io.whitefox.api.deltasharing.model.v1.Format;
import io.whitefox.api.deltasharing.model.v1.parquet.ParquetFile;
import io.whitefox.api.deltasharing.model.v1.parquet.ParquetMetadata;
import io.whitefox.api.deltasharing.model.v1.parquet.ParquetProtocol;
import io.whitefox.core.services.ContentAndToken;
import io.whitefox.persistence.StorageManager;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

@QuarkusTest
@Tag("integration")
public class DeltaSharesApiImplTest implements OpenApiValidatorUtils {
  private static final StorageManager storageManager = SampleTables.createStorageManager();

  @BeforeAll
  public static void setup() {
    QuarkusMock.installMockForType(storageManager, StorageManager.class);
  }

  private final DeltaPageTokenEncoder encoder;
  private final ObjectMapper objectMapper;

  @Inject
  public DeltaSharesApiImplTest(DeltaPageTokenEncoder encoder, ObjectMapper objectMapper) {
    this.encoder = encoder;
    this.objectMapper = objectMapper;
  }

  @Test
  public void getUnknownShare() {
    given()
        .pathParam("share", "unknownKey")
        .when()
        .filter(deltaFilter)
        .get("delta-api/v1/shares/{share}")
        .then()
        .statusCode(Response.Status.NOT_FOUND.getStatusCode());
  }

  @Test
  public void listShares() {
    given()
        .queryParam("maxResults", 50)
        .queryParam("pageToken", encoder.encodePageToken(new ContentAndToken.Token(0)))
        .when()
        .filter(deltaFilter)
        .get("delta-api/v1/shares")
        .then()
        .statusCode(200)
        .body("items[0].name", is("name"))
        .body("items[0].id", is("key"))
        .body("items", is(hasSize(1)))
        .body("token", is(nullValue()));
  }

  @Test
  public void listSharesNoParams() {
    given()
        .when()
        .filter(deltaFilter)
        .get("delta-api/v1/shares")
        .then()
        .statusCode(200)
        .body("items[0].name", is("name"))
        .body("items[0].id", is("key"))
        .body("items", is(hasSize(1)))
        .body("token", is(nullValue()));
  }

  @Test
  public void listNotFoundSchemas() {
    given()
        .when()
        .filter(deltaFilter)
        .get("delta-api/v1/shares/{share}/schemas", "name1")
        .then()
        .statusCode(404)
        .body("errorCode", is("1"))
        .body("message", is("NOT FOUND"));
  }

  @Test
  public void listSchemas() {
    given()
        .when()
        .filter(deltaFilter)
        .get("delta-api/v1/shares/{share}/schemas", "name")
        .then()
        .statusCode(200)
        .body("items[0].name", is("default"))
        .body("items[0].share", is("name"))
        .body("nextPageToken", is(nullValue()));
  }

  @Test
  public void listNotExistingTablesInShare() {
    given()
        .when()
        .filter(deltaFilter)
        .get("delta-api/v1/shares/{share}/schemas/{schema}/tables", "name2", "default")
        .then()
        .statusCode(404)
        .body("errorCode", is("1"))
        .body("message", is("NOT FOUND"));
  }

  @Test
  public void listNotExistingTablesInSchema() {
    given()
        .when()
        .filter(deltaFilter)
        .get("delta-api/v1/shares/{share}/schemas/{schema}/tables", "name", "default2")
        .then()
        .statusCode(404)
        .body("errorCode", is("1"))
        .body("message", is("NOT FOUND"));
  }

  @Test
  public void listTables() {
    given()
        .when()
        .filter(deltaFilter)
        .get("delta-api/v1/shares/{share}/schemas/{schema}/tables", "name", "default")
        .then()
        .statusCode(200)
        .body("items", hasSize(4))
        .body(
            "items[0].name",
            either(is("table1"))
                .or(is("table-with-history"))
                .or(is("icebergtable1"))
                .or(is("icebergtable2")))
        .body("items[0].schema", is("default"))
        .body("items[0].share", is("name"))
        .body("nextPageToken", is(nullValue()));
  }

  @Test
  public void tableMetadataNotFound() {
    given()
        .when()
        .filter(deltaFilter)
        .get(
            "delta-api/v1/shares/{share}/schemas/{schema}/tables/{table}/metadata",
            "name",
            "default",
            "tableNameNotFound")
        .then()
        .statusCode(404);
  }

  @Test
  @DisabledOnOs(OS.WINDOWS)
  public void deltaTableMetadata() throws IOException {
    var responseBodyLines = given()
        .when()
        .filter(deltaFilter)
        .get(
            "delta-api/v1/shares/{share}/schemas/{schema}/tables/{table}/metadata",
            "name",
            "default",
            "table1")
        .then()
        .statusCode(200)
        .extract()
        .asString()
        .split("\n");
    assertEquals(2, responseBodyLines.length);
    assertEquals(
        ParquetProtocol.ofMinReaderVersion(1),
        objectMapper.reader().readValue(responseBodyLines[0], ParquetProtocol.class));
    assertEquals(
        ParquetMetadata.builder()
            .metadata(ParquetMetadata.Metadata.builder()
                .id("56d48189-cdbc-44f2-9b0e-2bded4c79ed7")
                .name(Optional.of("table1"))
                .format(new Format())
                .schemaString(
                    "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}")
                .partitionColumns(List.of())
                .version(Optional.of(0L))
                .configuration(Optional.of(Map.of()))
                .build())
            .build(),
        objectMapper.reader().readValue(responseBodyLines[1], ParquetMetadata.class));
  }

  /**
   * This test is disabled because we statically generate iceberg tables and then test against them.
   * Unfortunately iceberg table metadata contains the files full path, not a relative one, therefore if we generate it
   * on someone laptop it will not be able to find it during CI or on another person laptop.
   * This code is still tested against s3 so we're still covered.
   */
  @Test
  @Disabled
  public void icebergTableVersion() {
    given()
        .when()
        .filter(deltaFilter)
        .get(
            "delta-api/v1/shares/{share}/schemas/{schema}/tables/{table}/version",
            "name",
            "default",
            "icebergtable1")
        .then()
        .statusCode(200)
        .header("Delta-Table-Version", "1");
  }

  /**
   * This test is disabled because we statically generate iceberg tables and then test against them.
   * Unfortunately iceberg table metadata contains the files full path, not a relative one, therefore if we generate it
   * on someone laptop it will not be able to find it during CI or on another person laptop.
   * This code is still tested against s3 so we're still covered.
   */
  @Test
  @Disabled
  public void icebergTableMetadata() throws IOException {
    var responseBodyLines = given()
        .when()
        .filter(deltaFilter)
        .get(
            "delta-api/v1/shares/{share}/schemas/{schema}/tables/{table}/metadata",
            "name",
            "default",
            "icebergtable1")
        .then()
        .statusCode(200)
        .extract()
        .asString()
        .split("\n");
    assertEquals(2, responseBodyLines.length);
    assertEquals(
        ParquetProtocol.ofMinReaderVersion(1),
        objectMapper.reader().readValue(responseBodyLines[0], ParquetProtocol.class));
    assertEquals(
        ParquetMetadata.builder()
            .metadata(ParquetMetadata.Metadata.builder()
                .id("3369848726892806393")
                .name(Optional.of("metastore.test_db.icebergtable1"))
                .format(new Format())
                .schemaString(
                    "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}}]}")
                .partitionColumns(List.of())
                .version(Optional.of(1L))
                .configuration(Optional.of(Map.of("write.parquet.compression-codec", "zstd")))
                .build())
            .build(),
        objectMapper.reader().readValue(responseBodyLines[1], ParquetMetadata.class));
  }

  @Test
  public void listAllTables() {
    given()
        .when()
        .filter(deltaFilter)
        .get("delta-api/v1/shares/{share}/all-tables", "name")
        .then()
        .statusCode(200)
        .body("items", hasSize(4))
        .body(
            "items[0].name",
            either(is("table1"))
                .or(is("table-with-history"))
                .or(is("icebergtable1"))
                .or(is("icebergtable2")))
        .body("items[0].schema", is("default"))
        .body("items[0].share", is("name"))
        .body("nextPageToken", is(nullValue()));
  }

  @Test
  public void listAllOfMissingShare() {
    given()
        .when()
        .filter(deltaFilter)
        .get("delta-api/v1/shares/{share}/all-tables", "name2")
        .then()
        .statusCode(404);
  }

  @DisabledOnOs(OS.WINDOWS)
  @Test
  public void getTableVersion() {
    given()
        .when()
        .filter(deltaFilter)
        .get(
            "delta-api/v1/shares/{share}/schemas/{schema}/tables/{table}/version",
            "name",
            "default",
            "table1")
        .then()
        .statusCode(200)
        .header("Delta-Table-Version", is("0"));
  }

  @DisabledOnOs(OS.WINDOWS)
  @Test
  public void getTableVersionMissingTable() {
    given()
        .when()
        .filter(deltaFilter)
        .get(
            "delta-api/v1/shares/{share}/schemas/{schema}/tables/{table}/version",
            "name",
            "default",
            "table2")
        .then()
        .statusCode(404);
  }

  @DisabledOnOs(OS.WINDOWS)
  @Test
  public void getTableVersionNotFoundTimestamp() {
    given()
        .when()
        .filter(deltaFilter)
        .queryParam("startingTimestamp", "2024-10-20T10:15:30+01:00")
        .get(
            "delta-api/v1/shares/{share}/schemas/{schema}/tables/{table}/version",
            "name",
            "default",
            "table1")
        .then()
        .statusCode(404);
  }

  @DisabledOnOs(OS.WINDOWS)
  @Test
  public void getTableVersionBadTimestamp() {
    given()
        .when()
        .filter(deltaFilter)
        .queryParam("startingTimestamp", "acbsadqwafsdas")
        .get(
            "delta-api/v1/shares/{share}/schemas/{schema}/tables/{table}/version",
            "name",
            "default",
            "table1")
        .then()
        .statusCode(Response.Status.BAD_REQUEST.getStatusCode());
  }

  @DisabledOnOs(OS.WINDOWS)
  @Test
  public void queryNotExistingTable() throws IOException {
    given()
        .when()
        .filter(deltaFilter)
        .body("{}")
        .header(new Header("Content-Type", "application/json"))
        .post(
            "delta-api/v1/shares/{share}/schemas/{schema}/tables/{table}/query",
            "name",
            "default",
            "tableThatDoesNotExist")
        .then()
        .statusCode(404);
  }

  @DisabledOnOs(OS.WINDOWS)
  @Test
  public void queryTableCurrentVersionWithPredicates() throws IOException {
    var responseBodyLines = given()
        .when()
        .filter(deltaFilter)
        .body("{\"jsonPredicateHints\": \"{" + "  \\\"op\\\": \\\"equal\\\","
            + "  \\\"children\\\": ["
            + "    {\\\"op\\\": \\\"column\\\", \\\"name\\\":\\\"date\\\", \\\"valueType\\\":\\\"date\\\"},"
            + "    {\\\"op\\\":\\\"literal\\\",\\\"value\\\":\\\"2021-04-29\\\",\\\"valueType\\\":\\\"date\\\"}"
            + "  ]"
            + "}\"}")
        .header(new Header("Content-Type", "application/json"))
        .post(
            "delta-api/v1/shares/{share}/schemas/{schema}/tables/{table}/query",
            "name",
            "default",
            "table1")
        .then()
        .statusCode(200)
        .extract()
        .body()
        .asString()
        .split("\n");

    assertEquals(
        deltaTable1Protocol,
        objectMapper.reader().readValue(responseBodyLines[0], ParquetProtocol.class));
    assertEquals(
        deltaTable1Metadata,
        objectMapper.reader().readValue(responseBodyLines[1], ParquetMetadata.class));
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
    assertEquals(deltaTable1FilesWithoutPresignedUrl, files);
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
            "name",
            "default",
            "table1")
        .then()
        .statusCode(200)
        .header("Delta-Table-Version", "0")
        .extract()
        .body()
        .asString()
        .split("\n");

    assertEquals(
        deltaTable1Protocol,
        objectMapper.reader().readValue(responseBodyLines[0], ParquetProtocol.class));
    assertEquals(
        deltaTable1Metadata,
        objectMapper.reader().readValue(responseBodyLines[1], ParquetMetadata.class));
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
    assertEquals(deltaTable1FilesWithoutPresignedUrl, files); // TOD
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
            "name",
            "default",
            "table1")
        .then()
        .statusCode(200)
        .header("Delta-Table-Version", "0")
        .extract()
        .body()
        .asString()
        .split("\n");

    assertEquals(
        deltaTable1Protocol,
        objectMapper.reader().readValue(responseBodyLines[0], ParquetProtocol.class));
    assertEquals(
        deltaTable1Metadata,
        objectMapper.reader().readValue(responseBodyLines[1], ParquetMetadata.class));
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
    assertEquals(deltaTable1FilesWithoutPresignedUrl, files);
  }

  @Test
  @Disabled
  public void queryTableByTs() throws IOException {
    var responseBodyLines = given()
        .when()
        .filter(deltaFilter)
        .body("{\"timestamp\": \"2023-10-19T17:16:00Z\"}")
        .header(new Header("Content-Type", "application/json"))
        .post(
            "delta-api/v1/shares/{share}/schemas/{schema}/tables/{table}/query",
            "name",
            "default",
            "table-with-history")
        .then()
        .statusCode(200)
        .header("Delta-Table-Version", "0")
        .extract()
        .body()
        .asString()
        .split("\n");

    assertEquals(
        deltaTable1Protocol,
        objectMapper.reader().readValue(responseBodyLines[0], ParquetProtocol.class));
    assertEquals(
        deltaTableWithHistory1Metadata,
        objectMapper.reader().readValue(responseBodyLines[1], ParquetMetadata.class));
    assertDoesNotThrow(() -> Arrays.stream(responseBodyLines)
        .skip(2)
        .map(line -> {
          try {
            return objectMapper.reader().readValue(line, ParquetFile.class);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toSet()));
    assertEquals(7, responseBodyLines.length);
  }

  /**
   * This test is disabled because we statically generate iceberg tables and then test against them.
   * Unfortunately iceberg table metadata contains the files full path, not a relative one, therefore if we generate it
   * on someone laptop it will not be able to find it during CI or on another person laptop.
   * This code is still tested against s3 so we're still covered.
   */
  @Disabled
  @Test
  public void queryIcebergTableCurrentVersion() throws IOException {
    var responseBodyLines = given()
        .when()
        .filter(deltaFilter)
        .body("{}")
        .header(new Header("Content-Type", "application/json"))
        .post(
            "delta-api/v1/shares/{share}/schemas/{schema}/tables/{table}/query",
            "name",
            "default",
            "icebergtable1")
        .then()
        .statusCode(200)
        .header("Delta-Table-Version", "1")
        .extract()
        .body()
        .asString()
        .split("\n");

    assertEquals(
        localIcebergTable1Protocol,
        objectMapper.reader().readValue(responseBodyLines[0], ParquetProtocol.class));
    assertEquals(
        localIcebergTable1Metadata,
        objectMapper.reader().readValue(responseBodyLines[1], ParquetMetadata.class));
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
    assertEquals(localIcebergTableFilesToBeSigned, files);
  }

  /**
   * This test is disabled because we statically generate iceberg tables and then test against them.
   * Unfortunately iceberg table metadata contains the files full path, not a relative one, therefore if we generate it
   * on someone laptop it will not be able to find it during CI or on another person laptop.
   * This code is still tested against s3 so we're still covered.
   */
  @Disabled
  @Test
  public void queryIcebergTableByVersion() throws IOException {
    var responseBodyLines = given()
        .when()
        .filter(deltaFilter)
        .body("{\"version\": 3369848726892806393}")
        .header(new Header("Content-Type", "application/json"))
        .post(
            "delta-api/v1/shares/{share}/schemas/{schema}/tables/{table}/query",
            "name",
            "default",
            "icebergtable1")
        .then()
        .statusCode(200)
        .header("Delta-Table-Version", "1")
        .extract()
        .body()
        .asString()
        .split("\n");

    assertEquals(
        localIcebergTable1Protocol,
        objectMapper.reader().readValue(responseBodyLines[0], ParquetProtocol.class));
    assertEquals(
        localIcebergTable1Metadata,
        objectMapper.reader().readValue(responseBodyLines[1], ParquetMetadata.class));
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
    assertEquals(localIcebergTableFilesToBeSigned, files);
  }

  @Disabled
  @Test
  public void queryIcebergTableByTs() throws IOException {
    var responseBodyLines = given()
        .when()
        .filter(deltaFilter)
        .body("{\"timestamp\": \"2024-02-02T12:00:00Z\"}")
        .header(new Header("Content-Type", "application/json"))
        .post(
            "delta-api/v1/shares/{share}/schemas/{schema}/tables/{table}/query",
            "name",
            "default",
            "icebergtable1")
        .then()
        .statusCode(200)
        .header("Delta-Table-Version", "1")
        .extract()
        .body()
        .asString()
        .split("\n");

    assertEquals(
        localIcebergTable1Protocol,
        objectMapper.reader().readValue(responseBodyLines[0], ParquetProtocol.class));
    assertEquals(
        localIcebergTable1Metadata,
        objectMapper.reader().readValue(responseBodyLines[1], ParquetMetadata.class));
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
    assertEquals(localIcebergTableFilesToBeSigned, files);
  }
}
