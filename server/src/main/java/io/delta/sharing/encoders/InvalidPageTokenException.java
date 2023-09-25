package io.delta.sharing.encoders;

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
