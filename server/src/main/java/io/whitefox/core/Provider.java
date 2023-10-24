package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import io.whitefox.core.services.exceptions.TableAlreadyExists;
import java.util.*;

public class Provider {
  private final String name;
  private final Storage storage;
  private final Optional<Metastore> metastore;
  private final Long createdAt;
  private final Principal createdBy;
  private final Long updatedAt;
  private final Principal updatedBy;
  private final Principal owner;
  private final Map<String, InternalTable> tables;

  public Provider(
      String name,
      Storage storage,
      Optional<Metastore> metastore,
      Long createdAt,
      Principal createdBy,
      Long updatedAt,
      Principal updatedBy,
      Principal owner) {
    this.name = name;
    this.storage = storage;
    this.metastore = metastore;
    this.createdAt = createdAt;
    this.createdBy = createdBy;
    this.updatedAt = updatedAt;
    this.updatedBy = updatedBy;
    this.owner = owner;
    this.tables = Map.of();
  }

  public Provider(
      String name,
      Storage storage,
      Optional<Metastore> metastore,
      Long createdAt,
      Principal createdBy,
      Long updatedAt,
      Principal updatedBy,
      Principal owner,
      Map<String, InternalTable> tables) {
    this.name = name;
    this.storage = storage;
    this.metastore = metastore;
    this.createdAt = createdAt;
    this.createdBy = createdBy;
    this.updatedAt = updatedAt;
    this.updatedBy = updatedBy;
    this.owner = owner;
    this.tables = tables;
  }

  public Provider addTable(InternalTable table) {
    if (tables.containsKey(table.name())) {
      throw new TableAlreadyExists("Table " + table.name() + " already exists");
    }
    var newMap = new HashMap<String, InternalTable>();
    newMap.putAll(tables);
    newMap.put(table.name(), table);
    return new Provider(
        name,
        storage,
        metastore,
        createdAt,
        createdBy,
        updatedAt,
        updatedBy,
        owner,
        Collections.unmodifiableMap(newMap));
  }

  public String name() {
    return name;
  }

  public Storage storage() {
    return storage;
  }

  public Optional<Metastore> metastore() {
    return metastore;
  }

  public Long createdAt() {
    return createdAt;
  }

  public Principal createdBy() {
    return createdBy;
  }

  public Long updatedAt() {
    return updatedAt;
  }

  public Principal updatedBy() {
    return updatedBy;
  }

  public Principal owner() {
    return owner;
  }

  public Map<String, InternalTable> tables() {
    return tables;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Provider provider = (Provider) o;
    return Objects.equals(name, provider.name)
        && Objects.equals(storage, provider.storage)
        && Objects.equals(metastore, provider.metastore)
        && Objects.equals(createdAt, provider.createdAt)
        && Objects.equals(createdBy, provider.createdBy)
        && Objects.equals(updatedAt, provider.updatedAt)
        && Objects.equals(updatedBy, provider.updatedBy)
        && Objects.equals(owner, provider.owner);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(
        name, storage, metastore, createdAt, createdBy, updatedAt, updatedBy, owner);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "Provider{" + "name='"
        + name + '\'' + ", storage="
        + storage + ", metastore="
        + metastore + ", createdAt="
        + createdAt + ", createdBy="
        + createdBy + ", updatedAt="
        + updatedAt + ", updatedBy="
        + updatedBy + ", owner="
        + owner + '}';
  }
}
