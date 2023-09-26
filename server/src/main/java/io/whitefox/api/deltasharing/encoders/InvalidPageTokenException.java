package io.whitefox.api.deltasharing.encoders;

public class InvalidPageTokenException extends RuntimeException {
  public InvalidPageTokenException(Throwable cause) {
    super(cause);
  }

  public InvalidPageTokenException(String message) {
    super(message);
  }

  public InvalidPageTokenException(String message, Throwable cause) {
    super(message, cause);
  }
}
