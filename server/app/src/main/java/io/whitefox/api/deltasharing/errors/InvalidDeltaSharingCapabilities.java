package io.whitefox.api.deltasharing.errors;

public class InvalidDeltaSharingCapabilities extends IllegalArgumentException {
  public InvalidDeltaSharingCapabilities(String message) {
    super(message);
  }
}
