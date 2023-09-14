package io.lake.sharing.api.server;

import static io.restassured.RestAssured.given;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class SharesApiImplTest {

  @Test
  public void testShareEndpoint() {
    given().when().get("/shares").then().statusCode(200);
  }

  @Test
  public void testlistTablesInSchemaEndpoint() {
    given().when().get("/shares/share1/schema1/tables").then().statusCode(200);
  }
}
