package io.whitefox.core.storage;

import io.whitefox.annotations.SkipCoverageGenerated;
import io.whitefox.core.AwsCredentials;
import io.whitefox.core.Principal;
import java.util.Objects;
import java.util.Optional;

public final class CreateStorage {
  private final String name;
  private final Optional<String> comment;
  private final StorageType type;
  private final Principal currentUser;
  private final AwsCredentials credentials;
  private final String uri;
  private final Boolean skipValidation;

  public CreateStorage(
      String name,
      Optional<String> comment,
      StorageType type,
      Principal currentUser,
      AwsCredentials credentials,
      String uri,
      Boolean skipValidation) {
    this.name = name;
    this.comment = comment;
    this.type = type;
    this.currentUser = currentUser;
    this.credentials = credentials;
    this.uri = uri;
    this.skipValidation = skipValidation;
  }

  public String name() {
    return name;
  }

  public Optional<String> comment() {
    return comment;
  }

  public StorageType type() {
    return type;
  }

  public AwsCredentials credentials() {
    return credentials;
  }

  public String uri() {
    return uri;
  }

  public Boolean skipValidation() {
    return skipValidation;
  }

  public Principal currentUser() {
    return currentUser;
  }

  @SkipCoverageGenerated
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateStorage that = (CreateStorage) o;
    return Objects.equals(name, that.name)
        && Objects.equals(comment, that.comment)
        && type == that.type
        && Objects.equals(currentUser, that.currentUser)
        && Objects.equals(credentials, that.credentials)
        && Objects.equals(uri, that.uri)
        && Objects.equals(skipValidation, that.skipValidation);
  }

  @SkipCoverageGenerated
  @Override
  public int hashCode() {
    return Objects.hash(name, comment, type, currentUser, credentials, uri, skipValidation);
  }
}
