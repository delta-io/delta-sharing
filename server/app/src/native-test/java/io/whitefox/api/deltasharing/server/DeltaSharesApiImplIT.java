package io.whitefox.api.deltasharing.server;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.whitefox.api.OpenApiValidatorUtils;
import io.whitefox.api.deltasharing.encoders.DeltaPageTokenEncoder;
import io.whitefox.core.services.ContentAndToken;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
public class DeltaSharesApiImplIT implements OpenApiValidatorUtils {

  private final DeltaPageTokenEncoder encoder = new DeltaPageTokenEncoder();

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
        .body("items", hasSize(0))
        .body("nextPageToken", is(nullValue()));
  }

  @Test
  public void listSharesNoParams() {
    given()
        .when()
        .filter(deltaFilter)
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
        .filter(deltaFilter)
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
}
