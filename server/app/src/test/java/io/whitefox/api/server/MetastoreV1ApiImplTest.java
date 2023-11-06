package io.whitefox.api.server;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.Header;
import io.restassured.internal.mapping.Jackson2Mapper;
import io.whitefox.api.OpenApiValidatorUtils;
import io.whitefox.api.model.v1.generated.CreateMetastore;
import io.whitefox.api.model.v1.generated.MetastoreProperties;
import io.whitefox.api.model.v1.generated.SimpleAwsCredentials;
import io.whitefox.persistence.StorageManager;
import io.whitefox.persistence.memory.InMemoryStorageManager;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.jupiter.api.*;

@QuarkusTest
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MetastoreV1ApiImplTest implements OpenApiValidatorUtils {

  @BeforeAll
  public static void setup() {
    QuarkusMock.installMockForType(new InMemoryStorageManager(), StorageManager.class);
    QuarkusMock.installMockForType(
        Clock.fixed(Instant.ofEpochMilli(0L), ZoneId.of("UTC")), Clock.class);
  }

  @Inject
  private ObjectMapper objectMapper;

  private final CreateMetastore createMetastore = new CreateMetastore()
      .name("glue_metastore_prod")
      .skipValidation(false)
      .type(CreateMetastore.TypeEnum.GLUE)
      .properties(new MetastoreProperties()
          .catalogId("123")
          .credentials(new SimpleAwsCredentials()
              .awsAccessKeyId("access")
              .awsSecretAccessKey("secret")
              .region("eu-west1")));

  @Test
  @Order(0)
  public void createFirstMetastore() {
    given()
        .when()
        .filter(whitefoxFilter)
        .body(createMetastore, new Jackson2Mapper((cls, charset) -> objectMapper))
        .header(new Header("Content-Type", "application/json"))
        .post("/whitefox-api/v1/metastores")
        .then()
        .statusCode(201)
        .body("name", is(createMetastore.getName()))
        .body("owner", is("Mr. Fox"))
        .body("type", is(createMetastore.getType().value()))
        .body("properties.catalogId", is(createMetastore.getProperties().getCatalogId()))
        .body(
            "properties.credentials.awsAccessKeyId",
            is(createMetastore.getProperties().getCredentials().getAwsAccessKeyId()))
        .body(
            "properties.credentials.awsSecretAccessKey",
            is(createMetastore.getProperties().getCredentials().getAwsSecretAccessKey()))
        .body(
            "properties.credentials.region",
            is(createMetastore.getProperties().getCredentials().getRegion()))
        .body("validatedAt", is(0))
        .body("createdAt", is(0))
        .body("updatedBy", is("Mr. Fox"))
        .body("updatedAt", is(0));
  }

  @Test
  @Order(1)
  public void failToCreateSameMetastore() {
    given()
        .when()
        .filter(whitefoxFilter)
        .body(createMetastore, new Jackson2Mapper((cls, charset) -> objectMapper))
        .header(new Header("Content-Type", "application/json"))
        .post("/whitefox-api/v1/metastores")
        .then()
        .statusCode(409)
        .body("errorCode", is("CONFLICT"));
    ;
  }

  @Test
  @Order(1)
  public void getMetastore() {
    given()
        .when()
        .filter(whitefoxFilter)
        .get("/whitefox-api/v1/metastores/{name}", createMetastore.getName())
        .then()
        .statusCode(200)
        .body("name", is(createMetastore.getName()))
        .body("owner", is("Mr. Fox"))
        .body("type", is(createMetastore.getType().value()))
        .body("properties.catalogId", is(createMetastore.getProperties().getCatalogId()))
        .body(
            "properties.credentials.awsAccessKeyId",
            is(createMetastore.getProperties().getCredentials().getAwsAccessKeyId()))
        .body(
            "properties.credentials.awsSecretAccessKey",
            is(createMetastore.getProperties().getCredentials().getAwsSecretAccessKey()))
        .body(
            "properties.credentials.region",
            is(createMetastore.getProperties().getCredentials().getRegion()))
        .body("validatedAt", is(0))
        .body("createdAt", is(0))
        .body("updatedBy", is("Mr. Fox"))
        .body("updatedAt", is(0));
  }

  @Test
  @Order(1)
  public void notExistingMetastore() {
    given()
        .when()
        .filter(whitefoxFilter)
        .get("/whitefox-api/v1/metastores/{name}", "fake")
        .then()
        .statusCode(404)
        .body("message", is("NOT FOUND"));
  }
}
