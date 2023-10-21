package io.whitefox.api.server;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.Header;
import io.restassured.internal.mapping.Jackson2Mapper;
import io.whitefox.OpenApiValidationFilter;
import io.whitefox.api.model.v1.generated.*;
import io.whitefox.persistence.StorageManager;
import io.whitefox.persistence.memory.InMemoryStorageManager;
import jakarta.inject.Inject;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.jupiter.api.*;

@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ProviderV1ApiImplTest {

  @BeforeAll
  public static void setup() {
    QuarkusMock.installMockForType(new InMemoryStorageManager(), StorageManager.class);
    QuarkusMock.installMockForType(
        Clock.fixed(Instant.ofEpochMilli(0L), ZoneId.of("UTC")), Clock.class);
  }

  private static final String specLocation = Paths.get(".")
      .toAbsolutePath()
      .getParent()
      .getParent()
      .resolve("docs/protocol/whitefox-protocol-api.yml")
      .toAbsolutePath()
      .toString();
  private static final OpenApiValidationFilter filter = new OpenApiValidationFilter(specLocation);

  @Inject
  private ObjectMapper objectMapper;

  private final CreateStorage createStorage(String name) {
    return new CreateStorage()
        .name(name)
        .skipValidation(false)
        .credentials(new StorageCredentials()
            .awsAccessKeyId("accessKey")
            .awsSecretAccessKey("secretKey")
            .region("eu-east-1"))
        .uri("s3://bucket/storage")
        .type(CreateStorage.TypeEnum.S3);
  }

  private final CreateMetastore createMetastore(String name) {
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
    var storage = createStorage(storageName);
    var providerName = "provider1";
    given()
        .when()
        .filter(filter)
        .body(storage, new Jackson2Mapper((cls, charset) -> objectMapper))
        .header(new Header("Content-Type", "application/json"))
        .post("/whitefox-api/v1/storage")
        .then()
        .statusCode(201);
    given()
        .when()
        .filter(filter)
        .body(
            new ProviderInput().storageName(storageName).name(providerName),
            new Jackson2Mapper((cls, charset) -> objectMapper))
        .header(new Header("Content-Type", "application/json"))
        .post("/whitefox-api/v1/providers")
        .then()
        .statusCode(200)
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
    var storage = createStorage(storageName);
    var metastoreName = "metastore";
    var metastore = createMetastore(metastoreName);
    var providerName = "provider2";
    given()
        .when()
        .filter(filter)
        .body(storage, new Jackson2Mapper((cls, charset) -> objectMapper))
        .header(new Header("Content-Type", "application/json"))
        .post("/whitefox-api/v1/storage")
        .then()
        .statusCode(201);
    given()
        .when()
        .filter(filter)
        .body(metastore, new Jackson2Mapper((cls, charset) -> objectMapper))
        .header(new Header("Content-Type", "application/json"))
        .post("/whitefox-api/v1/metastores")
        .then()
        .statusCode(201);
    given()
        .when()
        .filter(filter)
        .body(
            new ProviderInput()
                .storageName(storageName)
                .metastoreName(metastoreName)
                .name(providerName),
            new Jackson2Mapper((cls, charset) -> objectMapper))
        .header(new Header("Content-Type", "application/json"))
        .post("/whitefox-api/v1/providers")
        .then()
        .statusCode(200)
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

  @Test
  @Order(1)
  public void failToCreateSameProvider() {
    var providerName = "provider1";
    given()
        .when()
        .filter(filter)
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
        .filter(filter)
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
        .filter(filter)
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
        .filter(filter)
        .get("/whitefox-api/v1/providers/{name}", "fake")
        .then()
        .body("message", is("NOT FOUND"))
        .statusCode(404);
  }
}
