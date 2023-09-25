package io.delta.sharing.api;

import java.util.Objects;
import java.util.Optional;

public class ContentAndToken<A> {
  private final A content;
  private final String token;

  public static final class Token {
    public final String value;

    public Token(String value) {
      this.value = value;
    }

    public static Token of(String value) {
      return new Token(value);
    }
  }

  private ContentAndToken(A content, String token) {
    this.content = content;
    this.token = token;
  }

  public static <T> ContentAndToken<T> withoutToken(T content) {
    Objects.requireNonNull(content);
    return new ContentAndToken<T>(content, null);
  }

  public static <T> ContentAndToken<T> of(T content, String token) {
    Objects.requireNonNull(content);
    Objects.requireNonNull(token);
    return new ContentAndToken<T>(content, token);
  }

  public Optional<A> getContent() {
    return Optional.ofNullable(content);
  }

  public Optional<String> getToken() {
    return Optional.ofNullable(token);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ContentAndToken<?> that = (ContentAndToken<?>) o;
    return Objects.equals(content, that.content) && Objects.equals(token, that.token);
  }

  @Override
  public int hashCode() {
    return Objects.hash(content, token);
  }
}
