package io.whitefox.core.types.predicates;

public enum EvaluatorVersion {
  V1("v1"),
  V2("v2");

  public final String value;

  EvaluatorVersion(String value) {
    this.value = value;
  }
}
