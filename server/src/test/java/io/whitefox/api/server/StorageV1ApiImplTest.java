package io.whitefox.api.server;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.Header;
import io.restassured.internal.mapping.Jackson2Mapper;
import io.whitefox.OpenApiValidationFilter;
import io.whitefox.api.model.v1.generated.CreateStorage;
import io.whitefox.api.model.v1.generated.StorageCredentials;
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
public class StorageV1ApiImplTest {

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

  private final CreateStorage createStorage = new CreateStorage()
      .name("s3_storage_prod")
      .skipValidation(false)
      .credentials(new StorageCredentials()
          .awsAccessKeyId("accessKey")
          .awsSecretAccessKey("secretKey")
          .region("eu-east-1"))
      .uri("s3://bucket/storage")
      .type(CreateStorage.TypeEnum.S3);

  @Test
  @Order(0)
  public void createFirstStorage() {
    given()
        .when()
        .filter(filter)
        .body(createStorage, new Jackson2Mapper((cls, charset) -> objectMapper))
        .header(new Header("Content-Type", "application/json"))
        .post("/whitefox-api/v1/storage")
        .then()
        .statusCode(201)
        .body("name", is(createStorage.getName()))
        .body("owner", is("Mr. Fox"))
        .body("uri", is("s3://bucket/storage"))
        .body("type", is(createStorage.getType().value()))
        .body("validatedAt", is(0))
        .body("createdAt", is(0))
        .body("updatedBy", is("Mr. Fox"))
        .body("updatedAt", is(0));
  }

  @Test
  @Order(1)
  public void failToCreateSameStorage() {
    given()
        .when()
        .filter(filter)
        .body(createStorage, new Jackson2Mapper((cls, charset) -> objectMapper))
        .header(new Header("Content-Type", "application/json"))
        .post("/whitefox-api/v1/storage")
        .then()
        .statusCode(409)
        .body("errorCode", is("CONFLICT"));
    ;
  }

  @Test
  @Order(1)
  public void getStorage() {
    given()
        .when()
        .filter(filter)
        .get("/whitefox-api/v1/storage/{name}", createStorage.getName())
        .then()
        .statusCode(200)
        .body("name", is(createStorage.getName()))
        .body("owner", is("Mr. Fox"))
        .body("type", is(createStorage.getType().value()))
        .body("validatedAt", is(0))
        .body("createdAt", is(0))
        .body("updatedBy", is("Mr. Fox"))
        .body("updatedAt", is(0));
  }

  @Test
  @Order(1)
  public void notExistingStorage() {
    given()
        .when()
        .filter(filter)
        .get("/whitefox-api/v1/storage/{name}", "fake")
        .then()
        .statusCode(404)
        .body("message", is("NOT FOUND"));
  }
}
