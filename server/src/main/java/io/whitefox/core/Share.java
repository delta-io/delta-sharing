package io.whitefox.core;

import java.util.Map;
import java.util.Objects;

public final class Share {
  private final String name;
  private final String id;
  private final Map<String, Schema> schemas;

  public Share(String name, String id, Map<String, Schema> schemas) {
    this.name = name;
    this.id = id;
    this.schemas = schemas;
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

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (Share) obj;
    return Objects.equals(this.name, that.name)
        && Objects.equals(this.id, that.id)
        && Objects.equals(this.schemas, that.schemas);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, id, schemas);
  }

  @Override
  public String toString() {
    return "Share[" + "name=" + name + ", " + "id=" + id + ", " + "schemas=" + schemas + ']';
  }
}
