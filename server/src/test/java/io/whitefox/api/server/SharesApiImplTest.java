package io.whitefox.api.server;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class SharesApiImplTest {

  @Test
  public void testlistTablesInSchemaEndpoint() {
    RestAssured.given().when().get("/shares/share1/schema1/tables").then().statusCode(200);
  }
}
