package io.whitefox.api.server;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.Header;
import io.restassured.internal.mapping.Jackson2Mapper;
import io.restassured.response.ValidatableResponse;
import io.whitefox.api.deltasharing.OpenApiValidatorUtils;
import io.whitefox.api.model.v1.generated.*;
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
public class ProviderV1ApiImplTest implements OpenApiValidatorUtils {

  @BeforeAll
  public static void setup() {
    QuarkusMock.installMockForType(new InMemoryStorageManager(), StorageManager.class);
    QuarkusMock.installMockForType(
        Clock.fixed(Instant.ofEpochMilli(0L), ZoneId.of("UTC")), Clock.class);
  }

  @Inject
  private ObjectMapper objectMapper;

  public static CreateStorage createStorageObj(String name) {
    return new CreateStorage()
        .name(name)
        .skipValidation(false)
        .properties(new StorageProperties()
            .credentials(new SimpleAwsCredentials()
                .awsAccessKeyId("accessKey")
                .awsSecretAccessKey("secretKey")
                .region("eu-east-1")))
        .uri("s3://bucket/storage")
        .type(CreateStorage.TypeEnum.S3);
  }

  public static CreateMetastore createMetastoreObj(String name) {
    return new CreateMetastore()
        .name(name)
        .skipValidation(false)
        .type(CreateMetastore.TypeEnum.GLUE)
        .properties(new MetastoreProperties()
            .catalogId("123")
            .credentials(new SimpleAwsCredentials()
                .awsAccessKeyId("access")
                .awsSecretAccessKey("secret")
                .region("eu-west1")));
  }

  @Test
  @Order(0)
  public void createFirstProviderWithoutMetastore() {
    var storageName = "storage1";
    var storage = createStorageObj(storageName);
    var providerName = "provider1";
    createStorageAction(storage, objectMapper);
    createProviderAction(storageName, Optional.empty(), providerName, objectMapper)
        .body("name", is(providerName))
        .body("owner", is("Mr. Fox"))
        .body("createdAt", is(0))
        .body("updatedBy", is("Mr. Fox"))
        .body("updatedAt", is(0))
        .body("storage.name", is(storage.getName()))
        .body("storage.owner", is("Mr. Fox"))
        .body("storage.uri", is("s3://bucket/storage"))
        .body("storage.type", is(storage.getType().value()))
        .body("storage.validatedAt", is(0))
        .body("storage.createdAt", is(0))
        .body("storage.updatedBy", is("Mr. Fox"))
        .body("storage.updatedAt", is(0));
  }

  @Test
  @Order(0)
  public void createFirstProviderWithMetastore() {
    var storageName = "storage2";
    var storage = createStorageObj(storageName);
    var metastoreName = "metastore";
    var metastore = createMetastoreObj(metastoreName);
    var providerName = "provider2";
    createStorageAction(storage, objectMapper);
    createMetastoreAction(metastore, objectMapper);
    createProviderAction(storageName, Optional.of(metastoreName), providerName, objectMapper)
        .body("name", is(providerName))
        .body("owner", is("Mr. Fox"))
        .body("createdAt", is(0))
        .body("updatedBy", is("Mr. Fox"))
        .body("updatedAt", is(0))
        .body("storage.name", is(storage.getName()))
        .body("storage.owner", is("Mr. Fox"))
        .body("storage.uri", is("s3://bucket/storage"))
        .body("storage.type", is(storage.getType().value()))
        .body("storage.validatedAt", is(0))
        .body("storage.createdAt", is(0))
        .body("storage.updatedBy", is("Mr. Fox"))
        .body("storage.updatedAt", is(0))
        .body("metastore.name", is(metastore.getName()))
        .body("metastore.owner", is("Mr. Fox"))
        .body("metastore.type", is(metastore.getType().value()))
        .body("metastore.properties.catalogId", is(metastore.getProperties().getCatalogId()))
        .body(
            "metastore.properties.credentials.awsAccessKeyId",
            is(metastore.getProperties().getCredentials().getAwsAccessKeyId()))
        .body(
            "metastore.properties.credentials.awsSecretAccessKey",
            is(metastore.getProperties().getCredentials().getAwsSecretAccessKey()))
        .body(
            "metastore.properties.credentials.region",
            is(metastore.getProperties().getCredentials().getRegion()))
        .body("metastore.validatedAt", is(0))
        .body("metastore.createdAt", is(0))
        .body("metastore.updatedBy", is("Mr. Fox"))
        .body("metastore.updatedAt", is(0));
  }

  public static ValidatableResponse createProviderAction(
      String storageName,
      Optional<String> metastoreName,
      String providerName,
      ObjectMapper objectMapper) {
    return given()
        .when()
        .filter(whitefoxFilter)
        .body(
            new ProviderInput()
                .storageName(storageName)
                .metastoreName(metastoreName.orElse(null))
                .name(providerName),
            new Jackson2Mapper((cls, charset) -> objectMapper))
        .header(new Header("Content-Type", "application/json"))
        .post("/whitefox-api/v1/providers")
        .then()
        .statusCode(200);
  }

  public static ValidatableResponse createMetastoreAction(
      CreateMetastore metastore, ObjectMapper objectMapper) {
    return given()
        .when()
        .filter(whitefoxFilter)
        .body(metastore, new Jackson2Mapper((cls, charset) -> objectMapper))
        .header(new Header("Content-Type", "application/json"))
        .post("/whitefox-api/v1/metastores")
        .then()
        .statusCode(201);
  }

  public static ValidatableResponse createStorageAction(
      CreateStorage storage, ObjectMapper objectMapper) {
    return given()
        .when()
        .filter(whitefoxFilter)
        .body(storage, new Jackson2Mapper((cls, charset) -> objectMapper))
        .header(new Header("Content-Type", "application/json"))
        .post("/whitefox-api/v1/storage")
        .then()
        .statusCode(201);
  }

  @Test
  @Order(1)
  public void failToCreateSameProvider() {
    var providerName = "provider1";
    given()
        .when()
        .filter(whitefoxFilter)
        .body(
            new ProviderInput().storageName("storage1").name(providerName),
            new Jackson2Mapper((cls, charset) -> objectMapper))
        .header(new Header("Content-Type", "application/json"))
        .post("/whitefox-api/v1/providers")
        .then()
        .statusCode(409)
        .body("errorCode", is("CONFLICT"));
  }

  @Test
  @Order(1)
  public void failToCreateStorageNotFound() {
    var providerName = "provider2";
    given()
        .when()
        .filter(whitefoxFilter)
        .body(
            new ProviderInput().storageName("other").name(providerName),
            new Jackson2Mapper((cls, charset) -> objectMapper))
        .header(new Header("Content-Type", "application/json"))
        .post("/whitefox-api/v1/providers")
        .then()
        .statusCode(404)
        .body("errorCode", is("NOT FOUND"));
  }

  @Test
  @Order(1)
  public void getProvider() {
    var providerName = "provider1";
    given()
        .when()
        .filter(whitefoxFilter)
        .get("/whitefox-api/v1/providers/{name}", providerName)
        .then()
        .statusCode(200)
        .body("name", is(providerName))
        .body("owner", is("Mr. Fox"))
        .body("createdAt", is(0))
        .body("updatedBy", is("Mr. Fox"))
        .body("updatedAt", is(0))
        .body("storage.name", is("storage1"))
        .body("storage.owner", is("Mr. Fox"))
        .body("storage.uri", is("s3://bucket/storage"))
        .body("storage.type", is("s3"))
        .body("storage.validatedAt", is(0))
        .body("storage.createdAt", is(0))
        .body("storage.updatedBy", is("Mr. Fox"))
        .body("storage.updatedAt", is(0));
  }

  @Test
  @Order(1)
  public void notExistingProvider() {
    given()
        .when()
        .filter(whitefoxFilter)
        .get("/whitefox-api/v1/providers/{name}", "fake")
        .then()
        .body("message", is("NOT FOUND"))
        .statusCode(404);
  }
}
