package io.whitefox.api.deltasharing;

import io.whitefox.OpenApiValidationFilter;
import java.nio.file.Paths;

public interface OpenApiValidatorUtils {

  String deltaSpecLocation = Paths.get(".")
      .toAbsolutePath()
      .getParent()
      .getParent()
      .resolve("protocol/delta-sharing-protocol-api.yml")
      .toAbsolutePath()
      .toString();

  OpenApiValidationFilter deltaFilter = new OpenApiValidationFilter(deltaSpecLocation);

  String whitefoxSpecLocation = Paths.get(".")
      .toAbsolutePath()
      .getParent()
      .getParent()
      .resolve("protocol/whitefox-protocol-api.yml")
      .toAbsolutePath()
      .toString();

  OpenApiValidationFilter whitefoxFilter = new OpenApiValidationFilter(whitefoxSpecLocation);
}
