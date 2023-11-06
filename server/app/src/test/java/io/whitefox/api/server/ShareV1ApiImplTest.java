package io.whitefox.api.server;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import io.restassured.http.Header;
import io.restassured.internal.mapping.Jackson2Mapper;
import io.restassured.response.ValidatableResponse;
import io.whitefox.MutableClock;
import io.whitefox.api.OpenApiValidatorUtils;
import io.whitefox.api.model.v1.generated.AddRecipientToShareRequest;
import io.whitefox.api.model.v1.generated.AddTableToSchemaRequest;
import io.whitefox.api.model.v1.generated.CreateShareInput;
import io.whitefox.api.model.v1.generated.TableReference;
import io.whitefox.core.Principal;
import io.whitefox.core.services.ProviderService;
import io.whitefox.core.services.ShareServiceFixture;
import io.whitefox.core.services.StorageService;
import io.whitefox.core.services.TableService;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.List;
import org.junit.jupiter.api.*;

@QuarkusTest
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ShareV1ApiImplTest implements OpenApiValidatorUtils {

  private static final MutableClock clock = new MutableClock();

  @BeforeAll
  public static void setup() {
    QuarkusMock.installMockForType(clock, Clock.class);
  }

  @Inject
  private ObjectMapper objectMapper;

  @Inject
  private ProviderService providerService;

  @Inject
  private StorageService storageService;

  @Inject
  private TableService tableService;

  @Test
  @Order(0)
  void createShare() {
    createEmptyShare("share1")
        .statusCode(201)
        .body("name", is("share1"))
        .body("comment", is(nullValue()))
        .body("recipients", is(hasSize(0)))
        .body("schemas", is(hasSize(0)))
        .body("createdAt", is(0))
        .body("createdBy", is("Mr. Fox"))
        .body("updatedAt", is(0))
        .body("updatedBy", is("Mr. Fox"))
        .body("owner", is("Mr. Fox"));
    given()
        .when()
        .filter(deltaFilter)
        .get("delta-api/v1/shares")
        .then()
        .statusCode(200)
        .body("items", is(hasSize(1)))
        .body("items[0].name", is("share1"))
        .body("items[0].id", is("share1"))
        .body("token", is(nullValue()));
  }

  @Test
  @Order(1)
  void failToCreateShare() {
    createEmptyShare("share1").statusCode(409);
  }

  @Test
  @Order(2)
  void addRecipientsToShare() {
    addRecipientsToShare("share1", List.of("Antonio", "Marco", "Aleksandar"))
        .statusCode(200)
        .body("name", is("share1"))
        .body("comment", is(nullValue()))
        .body("recipients", is(hasSize(3)))
        .body("schemas", is(hasSize(0)))
        .body("createdAt", is(0))
        .body("createdBy", is("Mr. Fox"))
        .body("updatedAt", is(0))
        .body("updatedBy", is("Mr. Fox"))
        .body("owner", is("Mr. Fox"));
  }

  @Test
  @Order(3)
  void addSameRecipientTwice() {
    addRecipientsToShare("share1", List.of("Antonio"))
        .statusCode(200)
        .body("name", is("share1"))
        .body("comment", is(nullValue()))
        .body("recipients", is(hasSize(3)))
        .body("schemas", is(hasSize(0)))
        .body("createdAt", is(0))
        .body("createdBy", is("Mr. Fox"))
        .body("updatedAt", is(0))
        .body("updatedBy", is("Mr. Fox"))
        .body("owner", is("Mr. Fox"));
  }

  @Test
  @Order(4)
  void addAnotherRecipient() {
    addRecipientsToShare("share1", List.of("Paolo"))
        .statusCode(200)
        .body("name", is("share1"))
        .body("comment", is(nullValue()))
        .body("recipients", is(hasSize(4)))
        .body("schemas", is(hasSize(0)))
        .body("createdAt", is(0))
        .body("createdBy", is("Mr. Fox"))
        .body("updatedAt", is(0))
        .body("updatedBy", is("Mr. Fox"))
        .body("owner", is("Mr. Fox"));
  }

  @Test
  @Order(5)
  public void addRecipientToUnknownShare() {
    addRecipientsToShare("share2", List.of("Paolo")).statusCode(404);
  }

  @Test
  @Order(5)
  public void createSchema() {
    createSchemaInShare("share1", "schema1")
        .statusCode(201)
        .body("name", is("share1"))
        .body("comment", is(nullValue()))
        .body("recipients", is(hasSize(4)))
        .body("schemas", is(hasSize(1)))
        .body("schemas[0]", is("schema1"))
        .body("createdAt", is(0))
        .body("createdBy", is("Mr. Fox"))
        .body("updatedAt", is(0))
        .body("updatedBy", is("Mr. Fox"))
        .body("owner", is("Mr. Fox"));
  }

  @Test
  @Order(6)
  public void createSameSchema() {
    createSchemaInShare("share1", "schema1").statusCode(409);
  }

  @Test
  @Order(6)
  public void addTableToSchema() {
    clock.tickSeconds(10);
    var share = "share1";
    var schema = "schema1";
    ShareServiceFixture.setupInternalTable(
        storageService,
        providerService,
        tableService,
        new Principal("Mr. Fox"),
        "storage1",
        "provider1",
        "table1");
    given()
        .when()
        .filter(whitefoxFilter)
        .body(new AddTableToSchemaRequest()
            .name("shared1")
            .reference(new TableReference().providerName("provider1").name("table1")))
        .contentType(ContentType.JSON)
        .post("/whitefox-api/v1/shares/{share}/{schema}/tables", share, schema)
        .then()
        .statusCode(201)
        .body("name", is("share1"))
        .body("comment", is(nullValue()))
        .body("recipients", is(hasSize(4)))
        .body("schemas", is(hasSize(1)))
        .body("schemas[0]", is("schema1"))
        .body("createdAt", is(0))
        .body("createdBy", is("Mr. Fox"))
        .body("updatedAt", is(10000))
        .body("updatedBy", is("Mr. Fox"))
        .body("owner", is("Mr. Fox"));
  }

  ValidatableResponse createEmptyShare(String name) {
    return given()
        .when()
        .filter(whitefoxFilter)
        .body(
            new CreateShareInput().name(name).recipients(List.of()).schemas(List.of()),
            new Jackson2Mapper((cls, charset) -> objectMapper))
        .header(new Header("Content-Type", "application/json"))
        .post("/whitefox-api/v1/shares")
        .then();
  }

  ValidatableResponse createSchemaInShare(String share, String schema) {
    return given()
        .when()
        .filter(whitefoxFilter)
        .post("/whitefox-api/v1/shares/{share}/{schema}", share, schema)
        .then();
  }

  ValidatableResponse addRecipientsToShare(String share, List<String> recipients) {
    return given()
        .when()
        // .filter(wfFilter) // I need to disable the filter because it gets confused and
        // does not find the operation
        .body(
            new AddRecipientToShareRequest().principals(recipients),
            new Jackson2Mapper((cls, charset) -> objectMapper))
        .contentType(ContentType.JSON)
        .put("/whitefox-api/v1/shares/" + share + "/recipients")
        .then();
  }
}
