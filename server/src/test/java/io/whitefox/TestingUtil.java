package io.whitefox;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class TestingUtil {
  public static <T extends Throwable, U> Throwable assertFails(
      Class<T> expectedType, Supplier<CompletableFuture<U>> executable) {
    var exception = assertThrows(
        Exception.class, () -> executable.get().toCompletableFuture().get());
    var causedBy = exception.getCause();
    assertTrue(
        expectedType.isInstance(exception)
            || (exception instanceof ExecutionException && expectedType.isInstance(causedBy)),
        String.format(
            "expected to throw %s but %s and %s were thrown",
            expectedType, exception.getClass(), causedBy.getClass()));
    return exception;
  }
}
