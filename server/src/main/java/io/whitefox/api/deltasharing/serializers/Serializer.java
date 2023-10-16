package io.whitefox.api.deltasharing.serializers;

public interface Serializer<T> {

  String serialize(T data);
}
