package io.delta.sharing.api.server;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

import io.delta.sharing.encoders.DeltaPageTokenEncoder;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class DeltaSharesApiImplTest {

  @Inject DeltaPageTokenEncoder encoder;

  @Test
  public void getUnknownShare() {
    given()
        .pathParam("share", "unknownKey")
        .when()
        .get("/share/{share}")
        .then()
        .statusCode(Response.Status.NOT_FOUND.getStatusCode());
  }

  @Test
  public void listShares() {
    given()
        .queryParam("maxResults", 50.0)
        .queryParam("pageToken", encoder.encodePageToken("0"))
        .when()
        .get("/shares")
        .then()
        .statusCode(200)
        .body("items", is(empty()))
        .body("token", is(nullValue()));
  }

  @Test
  public void listSharesNoParams() {
    given()
        .when()
        .get("/shares")
        .then()
        .statusCode(200)
        .body("items", is(empty()))
        .body("token", is(nullValue()));
  }
}
