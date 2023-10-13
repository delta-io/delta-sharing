package io.whitefox.api.server;

import io.quarkus.runtime.util.ExceptionUtil;
import io.whitefox.api.deltasharing.model.v1.generated.CommonErrorResponse;
import io.whitefox.persistence.DuplicateKeyException;
import jakarta.ws.rs.core.Response;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public interface ApiUtils {
  Function<Throwable, Response> exceptionToResponse = t -> {
    if (t instanceof IllegalArgumentException) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new CommonErrorResponse()
              .errorCode("BAD REQUEST")
              .message(ExceptionUtil.generateStackTrace(t)))
          .build();
    } else if (t instanceof DuplicateKeyException) {
      return Response.status(Response.Status.CONFLICT)
          .entity(new CommonErrorResponse()
              .errorCode("CONFLICT")
              .message(ExceptionUtil.generateStackTrace(t)))
          .build();
    } else {
      return Response.status(Response.Status.BAD_GATEWAY)
          .entity(new CommonErrorResponse()
              .errorCode("BAD GATEWAY")
              .message(ExceptionUtil.generateStackTrace(t)))
          .build();
    }
  };

  default Response wrapExceptions(Supplier<Response> f, Function<Throwable, Response> mapper) {
    try {
      return f.get();
    } catch (Throwable t) {
      return mapper.apply(t);
    }
  }

  default Response notFoundResponse() {
    return Response.status(Response.Status.NOT_FOUND)
        .entity(new CommonErrorResponse().errorCode("1").message("NOT FOUND"))
        .build();
  }

  default <T> Response optionalToNotFound(Optional<T> opt, Function<T, Response> fn) {
    return opt.map(fn).orElse(notFoundResponse());
  }
}
