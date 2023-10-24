package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.*;

public final class Share {

  private final String name;
  private final String id;
  private final Map<String, Schema> schemas;
  private final Optional<String> comment;
  private final Set<Principal> recipients;
  private final long createdAt;
  private final Principal createdBy;
  private final long updatedAt;
  private final Principal updatedBy;
  private final Principal owner;

  public Share(
      String name,
      String id,
      Map<String, Schema> schemas,
      Optional<String> comment,
      Set<Principal> recipients,
      long createdAt,
      Principal createdBy,
      long updatedAt,
      Principal updatedBy,
      Principal owner) {
    this.name = name;
    this.id = id;
    this.schemas = schemas;
    this.comment = comment;
    this.recipients = recipients;
    this.createdAt = createdAt;
    this.createdBy = createdBy;
    this.updatedAt = updatedAt;
    this.updatedBy = updatedBy;
    this.owner = owner;
  }

  public Share(
      String name,
      String id,
      Map<String, Schema> schemas,
      Principal createPrincipal,
      long createTime) {
    this(
        name,
        id,
        schemas,
        Optional.empty(),
        Set.of(),
        createTime,
        createPrincipal,
        createTime,
        createPrincipal,
        createPrincipal);
  }

  public String name() {
    return name;
  }

  public String id() {
    return id;
  }

  public Map<String, Schema> schemas() {
    return schemas;
  }

  public Optional<String> comment() {
    return comment;
  }

  public Set<Principal> recipients() {
    return recipients;
  }

  public long createdAt() {
    return createdAt;
  }

  public Principal createdBy() {
    return createdBy;
  }

  public long updatedAt() {
    return updatedAt;
  }

  public Principal updatedBy() {
    return updatedBy;
  }

  public Principal owner() {
    return owner;
  }

  public Share addRecipients(List<Principal> recipients, Principal currentUser, long now) {
    var newRecipients = new HashSet<>(this.recipients);
    newRecipients.addAll(recipients);
    return new Share(
        name,
        id,
        schemas,
        comment,
        Set.copyOf(newRecipients),
        createdAt,
        createdBy,
        now,
        currentUser,
        owner);
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Share share = (Share) o;
    return createdAt == share.createdAt
        && updatedAt == share.updatedAt
        && Objects.equals(name, share.name)
        && Objects.equals(id, share.id)
        && Objects.equals(schemas, share.schemas)
        && Objects.equals(comment, share.comment)
        && Objects.equals(recipients, share.recipients)
        && Objects.equals(createdBy, share.createdBy)
        && Objects.equals(updatedBy, share.updatedBy)
        && Objects.equals(owner, share.owner);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(
        name, id, schemas, comment, recipients, createdAt, createdBy, updatedAt, updatedBy, owner);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "Share{" + "name='"
        + name + '\'' + ", id='"
        + id + '\'' + ", schemas="
        + schemas + ", comment="
        + comment + ", recipients="
        + recipients + ", createdAt="
        + createdAt + ", createdBy="
        + createdBy + ", updatedAt="
        + updatedAt + ", updatedBy="
        + updatedBy + ", owner="
        + owner + '}';
  }
}
