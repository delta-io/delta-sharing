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
import io.whitefox.api.deltasharing.model.v1.generated.*;
import io.whitefox.core.services.ContentAndToken;
import io.whitefox.persistence.StorageManager;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
        .body("items", hasSize(2))
        .body("items[0].name", either(is("table1")).or(is("table-with-history")))
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
  public void tableMetadata() throws IOException {
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
        new ProtocolObject().protocol(new ProtocolObjectProtocol().minReaderVersion(1)),
        objectMapper.reader().readValue(responseBodyLines[0], ProtocolObject.class));
    assertEquals(
        new MetadataObject()
            .metaData(new MetadataObjectMetaData()
                .id("56d48189-cdbc-44f2-9b0e-2bded4c79ed7")
                .name("table1")
                .format(new FormatObject().provider("parquet"))
                .schemaString(
                    "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}")
                .partitionColumns(List.of())
                .version(0L)
                ._configuration(Map.of())),
        objectMapper.reader().readValue(responseBodyLines[1], MetadataObject.class));
  }

  @Test
  public void listAllTables() {
    given()
        .when()
        .filter(deltaFilter)
        .get("delta-api/v1/shares/{share}/all-tables", "name")
        .then()
        .statusCode(200)
        .body("items", hasSize(2))
        .body("items[0].name", either(is("table1")).or(is("table-with-history")))
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
        .statusCode(502);
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
        .extract()
        .body()
        .asString()
        .split("\n");

    assertEquals(
        deltaTable1Protocol,
        objectMapper.reader().readValue(responseBodyLines[0], ProtocolObject.class));
    assertEquals(
        deltaTable1Metadata,
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
        .extract()
        .body()
        .asString()
        .split("\n");

    assertEquals(
        deltaTable1Protocol,
        objectMapper.reader().readValue(responseBodyLines[0], ProtocolObject.class));
    assertEquals(
        deltaTable1Metadata,
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
        .extract()
        .body()
        .asString()
        .split("\n");

    assertEquals(
        deltaTable1Protocol,
        objectMapper.reader().readValue(responseBodyLines[0], ProtocolObject.class));
    assertEquals(
        deltaTableWithHistory1Metadata,
        objectMapper.reader().readValue(responseBodyLines[1], MetadataObject.class));
    assertDoesNotThrow(() -> Arrays.stream(responseBodyLines)
        .skip(2)
        .map(line -> {
          try {
            return objectMapper.reader().readValue(line, FileObject.class);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toSet()));
    assertEquals(7, responseBodyLines.length);
  }
}
