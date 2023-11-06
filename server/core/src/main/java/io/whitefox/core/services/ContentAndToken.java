package io.whitefox.core.services;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Objects;
import java.util.Optional;

public class ContentAndToken<A> {
  private final A content;
  private final Token token;

  public static final class Token {
    private final int value;

    public Token(int value) {
      this.value = value;
    }

    public static Token of(Integer value) {
      return new Token(value);
    }

    public int value() {
      return value;
    }

    @Override
    @SkipCoverageGenerated
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (obj == null || obj.getClass() != this.getClass()) return false;
      var that = (Token) obj;
      return this.value == that.value;
    }

    @Override
    @SkipCoverageGenerated
    public int hashCode() {
      return Objects.hash(value);
    }

    @Override
    @SkipCoverageGenerated
    public String toString() {
      return "Token[" + "value=" + value + ']';
    }
  }

  private ContentAndToken(A content, Token token) {
    this.content = content;
    this.token = token;
  }

  public static <T> ContentAndToken<T> withoutToken(T content) {
    Objects.requireNonNull(content);
    return new ContentAndToken<T>(content, null);
  }

  public static <T> ContentAndToken<T> of(T content, Token token) {
    Objects.requireNonNull(content);
    Objects.requireNonNull(token);
    return new ContentAndToken<T>(content, token);
  }

  public A getContent() {
    return content;
  }

  public Optional<Token> getToken() {
    return Optional.ofNullable(token);
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ContentAndToken<?> that = (ContentAndToken<?>) o;
    return Objects.equals(content, that.content) && Objects.equals(token, that.token);
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(content, token);
  }
}
