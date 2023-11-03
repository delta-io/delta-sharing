package io.whitefox.api.server;

import static io.restassured.RestAssured.given;
import static io.whitefox.api.server.ProviderV1ApiImplTest.*;
import static org.hamcrest.Matchers.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.Header;
import io.restassured.internal.mapping.Jackson2Mapper;
import io.restassured.response.ValidatableResponse;
import io.whitefox.api.deltasharing.OpenApiValidatorUtils;
import io.whitefox.api.model.v1.generated.CreateTableInput;
import io.whitefox.core.InternalTable;
import io.whitefox.persistence.StorageManager;
import io.whitefox.persistence.memory.InMemoryStorageManager;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;
import org.junit.jupiter.api.*;

@QuarkusTest
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TableV1ApiImplTest implements OpenApiValidatorUtils {

  @BeforeAll
  public static void setup() {
    QuarkusMock.installMockForType(new InMemoryStorageManager(), StorageManager.class);
    QuarkusMock.installMockForType(
        Clock.fixed(Instant.ofEpochMilli(0L), ZoneId.of("UTC")), Clock.class);
  }

  @Inject
  private ObjectMapper objectMapper;

  @Test
  @Order(0)
  public void createFirstDeltaTable() {
    createStorageAction(createStorageObj("storage1"), objectMapper);
    createProviderAction("storage1", Optional.empty(), "provider1", objectMapper);
    var tableObj = createTableObj(
        "deltaTable", new InternalTable.DeltaTableProperties("file://some-path"), true);
    createTableAction(tableObj, "provider1", objectMapper)
        .statusCode(201)
        .body("providerName", is("provider1"))
        .body("name", is("deltaTable"))
        .body("properties.location", is("file://some-path"))
        .body("properties.type", is("delta"))
        .body("createdAt", is(0))
        .body("createdBy", is("Mr. Fox"))
        .body("updatedAt", is(0))
        .body("updatedBy", is("Mr. Fox"))
        .body("validateAt", nullValue())
        .body("validateBy", nullValue());
  }

  @Test
  @Order(1)
  public void createFirstIcebergTable() {
    createMetastoreAction(createMetastoreObj("metastore1"), objectMapper);
    createProviderAction("storage1", Optional.of("metastore1"), "provider2", objectMapper);
    var tableObj = createTableObj(
        "icebergTable", new InternalTable.IcebergTableProperties("db", "icebergTable"), true);
    createTableAction(tableObj, "provider2", objectMapper)
        .statusCode(201)
        .body("providerName", is("provider2"))
        .body("name", is("icebergTable"))
        .body("properties.databaseName", is("db"))
        .body("properties.tableName", is("icebergTable"))
        .body("properties.type", is("iceberg"))
        .body("createdAt", is(0))
        .body("createdBy", is("Mr. Fox"))
        .body("updatedAt", is(0))
        .body("updatedBy", is("Mr. Fox"))
        .body("validateAt", nullValue())
        .body("validateBy", nullValue());
  }

  @Test
  @Order(2)
  public void failToCreateExistingTables() {
    var tableObj1 = createTableObj(
        "icebergTable", new InternalTable.IcebergTableProperties("db", "icebergTable"), true);
    createTableAction(tableObj1, "provider2", objectMapper).statusCode(409);
    var tableObj2 = createTableObj(
        "deltaTable", new InternalTable.DeltaTableProperties("file://some-path"), true);
    createTableAction(tableObj2, "provider1", objectMapper).statusCode(409);
  }

  @Test
  @Order(2)
  public void failToCreateWhenProviderDoesNotExist() {
    var tableObj1 = createTableObj(
        "icebergTable", new InternalTable.IcebergTableProperties("db", "icebergTable"), true);
    createTableAction(tableObj1, "provider3", objectMapper).statusCode(404);
  }

  @Test
  @Order(2)
  public void describeExistingTable() {
    var providerName = "provider2";
    var tableName = "icebergTable";
    given()
        .when()
        .filter(whitefoxFilter)
        .get(
            "/whitefox-api/v1/providers/{providerName}/tables/{tableName}", providerName, tableName)
        .then()
        .statusCode(200)
        .body("providerName", is("provider2"))
        .body("name", is("icebergTable"))
        .body("properties.databaseName", is("db"))
        .body("properties.tableName", is("icebergTable"))
        .body("properties.type", is("iceberg"))
        .body("createdAt", is(0))
        .body("createdBy", is("Mr. Fox"))
        .body("updatedAt", is(0))
        .body("updatedBy", is("Mr. Fox"))
        .body("validateAt", nullValue())
        .body("validateBy", nullValue());
  }

  @Test
  @Order(2)
  public void describeNotExistingProvider() {
    var providerName = "provider3";
    var tableName = "icebergTable";
    given()
        .when()
        .filter(whitefoxFilter)
        .get(
            "/whitefox-api/v1/providers/{providerName}/tables/{tableName}", providerName, tableName)
        .then()
        .statusCode(404);
  }

  @Test
  @Order(2)
  public void describeNotExistingTableInProvider() {
    var providerName = "provider2";
    var tableName = "icebergTable2";
    given()
        .when()
        .filter(whitefoxFilter)
        .get(
            "/whitefox-api/v1/providers/{providerName}/tables/{tableName}", providerName, tableName)
        .then()
        .statusCode(404);
  }

  public static ValidatableResponse createTableAction(
      CreateTableInput createTableInput, String providerName, ObjectMapper objectMapper) {

    return given()
        .when()
        .filter(whitefoxFilter)
        .body(createTableInput, new Jackson2Mapper((cls, charset) -> objectMapper))
        .header(new Header("Content-Type", "application/json"))
        .post("/whitefox-api/v1/providers/{providerName}/tables", providerName)
        .then();
  }

  private static CreateTableInput createTableObj(
      String tableName, InternalTable.InternalTableProperties properties, boolean skipValidation) {
    return new CreateTableInput()
        .comment(null)
        .name(tableName)
        .properties(properties.asMap())
        .skipValidation(skipValidation);
  }
}
