package io.whitefox.api.utils;

public interface ScalaUtils {
  default <K, V> scala.collection.immutable.Map<K, V> emptyScalaMap() {
    return scala.collection.immutable.Map$.MODULE$.empty();
  }
}
