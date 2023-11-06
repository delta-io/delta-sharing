package io.whitefox.core.actions;

import io.whitefox.annotations.SkipCoverageGenerated;
import io.whitefox.core.Principal;
import java.util.Objects;
import java.util.Optional;

public class CreateProvider {
  private final String name;
  private final String storageName;
  private final Optional<String> metastoreName;
  private final Principal currentUser;

  public CreateProvider(
      String name, String storageName, Optional<String> metastoreName, Principal currentUser) {
    this.name = name;
    this.storageName = storageName;
    this.metastoreName = metastoreName;
    this.currentUser = currentUser;
  }

  public String name() {
    return name;
  }

  public String storageName() {
    return storageName;
  }

  public Optional<String> metastoreName() {
    return metastoreName;
  }

  public Principal currentUser() {
    return currentUser;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateProvider that = (CreateProvider) o;
    return Objects.equals(name, that.name)
        && Objects.equals(storageName, that.storageName)
        && Objects.equals(metastoreName, that.metastoreName)
        && Objects.equals(currentUser, that.currentUser);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(name, storageName, metastoreName, currentUser);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "CreateProvider{" + "name='"
        + name + '\'' + ", storageName='"
        + storageName + '\'' + ", metastoreName="
        + metastoreName + ", currentUser="
        + currentUser + '}';
  }
}
