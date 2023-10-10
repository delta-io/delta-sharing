package io.whitefox.api.deltasharing.server;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.whitefox.OpenApiValidationFilter;
import io.whitefox.api.deltasharing.encoders.DeltaPageTokenEncoder;
import io.whitefox.core.services.ContentAndToken;
import jakarta.ws.rs.core.Response;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
public class DeltaSharesApiImplIT {

  private final DeltaPageTokenEncoder encoder = new DeltaPageTokenEncoder();

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
        .queryParam("pageToken", encoder.encodePageToken(new ContentAndToken.Token(0)))
        .when()
        .filter(filter)
        .get("delta-api/v1/shares")
        .then()
        .statusCode(200)
        .body("items", hasSize(0))
        .body("nextPageToken", is(nullValue()));
  }

  @Test
  public void listSharesNoParams() {
    given()
        .when()
        .filter(filter)
        .get("delta-api/v1/shares")
        .then()
        .statusCode(200)
        .body("items", hasSize(0))
        .body("nextPageToken", is(nullValue()));
  }

  @Test
  public void listSchemas() {
    given()
        .when()
        .filter(filter)
        .get("delta-api/v1/shares/{share}/schemas", "name")
        .then()
        .statusCode(404)
        .body("errorCode", is("1"))
        .body("message", is("NOT FOUND"));
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
}
