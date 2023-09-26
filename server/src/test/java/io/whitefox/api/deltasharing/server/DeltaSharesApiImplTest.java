package io.whitefox.api.deltasharing.server;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.whitefox.OpenApiValidationFilter;
import io.whitefox.api.deltasharing.encoders.DeltaPageTokenEncoder;
import io.whitefox.persistence.StorageManager;
import io.whitefox.persistence.memory.InMemoryStorageManager;
import io.whitefox.persistence.memory.PSchema;
import io.whitefox.persistence.memory.PShare;
import io.whitefox.persistence.memory.PTable;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class DeltaSharesApiImplTest {

  @BeforeAll
  public static void setup() {
    var storageManager = new InMemoryStorageManager(List.of(new PShare(
        "name", "key", Map.of("default", new PSchema("default", List.of(new PTable("table1")))))));
    QuarkusMock.installMockForType(storageManager, StorageManager.class);
  }

  private final DeltaPageTokenEncoder encoder;

  @Inject
  public DeltaSharesApiImplTest(DeltaPageTokenEncoder encoder) {
    this.encoder = encoder;
  }

  private static final String specLocation = Paths.get(".")
      .toAbsolutePath()
      .getParent()
      .getParent()
      .resolve("docs/protocol/delta-sharing-protocol-api.yml")
      .toAbsolutePath()
      .toString();
  private static final OpenApiValidationFilter filter = new OpenApiValidationFilter(specLocation);

  @Test
  public void getUnknownShare() {
    given()
        .pathParam("share", "unknownKey")
        .when()
        .filter(filter)
        .get("delta-api/v1/shares/{share}")
        .then()
        .statusCode(Response.Status.NOT_FOUND.getStatusCode());
  }

  @Test
  public void listShares() {
    given()
        .queryParam("maxResults", 50)
        .queryParam("pageToken", encoder.encodePageToken("0"))
        .when()
        .filter(filter)
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
        .filter(filter)
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
        .filter(filter)
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
        .filter(filter)
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
        .filter(filter)
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
        .filter(filter)
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
        .filter(filter)
        .get("delta-api/v1/shares/{share}/schemas/{schema}/tables", "name", "default")
        .then()
        .statusCode(200)
        .body("items", hasSize(1))
        .body("items[0].name", is("table1"))
        .body("items[0].schema", is("default"))
        .body("items[0].share", is("name"))
        .body("nextPageToken", is(nullValue()));
  }
}
