package io.whitefox;

import io.restassured.filter.Filter;
import io.restassured.filter.FilterContext;
import io.restassured.response.Response;
import io.restassured.specification.FilterableRequestSpecification;
import io.restassured.specification.FilterableResponseSpecification;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.openapi4j.core.exception.ResolutionException;
import org.openapi4j.core.validation.ValidationException;
import org.openapi4j.core.validation.ValidationResults;
import org.openapi4j.operation.validator.adapters.server.restassured.RestAssuredRequest;
import org.openapi4j.operation.validator.adapters.server.restassured.RestAssuredResponse;
import org.openapi4j.operation.validator.util.PathResolver;
import org.openapi4j.operation.validator.validation.RequestValidator;
import org.openapi4j.parser.OpenApi3Parser;
import org.openapi4j.parser.model.v3.OpenApi3;
import org.openapi4j.parser.model.v3.Operation;
import org.openapi4j.parser.model.v3.Path;
import org.openapi4j.parser.validation.v3.OpenApi3Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenApiValidationFilter implements Filter {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenApiValidationFilter.class);

  private static final Map<String, Function<Path, Operation>> METHOD_TO_OPERATION = Map.of(
      "GET", Path::getGet,
      "PUT", Path::getPut,
      "POST", Path::getPost,
      "DELETE", Path::getDelete,
      "OPTIONS", Path::getOptions,
      "HEAD", Path::getHead,
      "PATCH", Path::getPatch,
      "TRACE", Path::getTrace);

  private final OpenApi3 api;

  public OpenApiValidationFilter(String yamlResourcePath) {
    this(parse(yamlResourcePath));
  }

  private static OpenApi3 parse(String yamlResourcePath) {
    try {
      return new OpenApi3Parser()
          .parse(java.nio.file.Path.of(yamlResourcePath).toUri().toURL(), true);
    } catch (ResolutionException | ValidationException | MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  public OpenApiValidationFilter(OpenApi3 api) {
    this.api = api;
  }

  @Override
  public Response filter(
      FilterableRequestSpecification requestSpec,
      FilterableResponseSpecification responseSpec,
      FilterContext ctx) {
    try {
      ValidationResults results = OpenApi3Validator.instance().validate(api);
      LOGGER.error("invalid OpenAPI definition: {}", results);

      RestAssuredRequest request = new RestAssuredRequest(requestSpec);
      RequestValidator validator = new RequestValidator(api);
      validator.validate(request);

      Response response = ctx.next(requestSpec, responseSpec);
      RestAssuredResponse responseWrapper = new RestAssuredResponse(response);
      Path path = getOpenApiPathByRequest(request);
      validator.validate(
          responseWrapper,
          path,
          METHOD_TO_OPERATION.get(requestSpec.getMethod()).apply(path));
      return response;
    } catch (ValidationException e) {
      throw new AssertionError(e);
    }
  }

  public Path getOpenApiPathByRequest(RestAssuredRequest request) {
    Map<Pattern, Path> patterns = new HashMap<>();
    for (Map.Entry<String, Path> pathEntry : api.getPaths().entrySet()) {
      List<Pattern> builtPathPatterns = PathResolver.instance()
          .buildPathPatterns(api.getContext(), api.getServers(), pathEntry.getKey());

      for (Pattern pathPattern : builtPathPatterns) {
        patterns.put(pathPattern, pathEntry.getValue());
      }
    }

    Pattern pathPattern =
        PathResolver.instance().findPathPattern(patterns.keySet(), request.getPath());
    Path p = patterns.get(pathPattern);
    return p;
  }
}
