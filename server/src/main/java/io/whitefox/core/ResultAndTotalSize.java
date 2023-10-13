package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Objects;

public final class ResultAndTotalSize<T> {
  private final T result;
  private final int size;

  public ResultAndTotalSize(T result, int size) {
    this.result = result;
    this.size = size;
  }

  public T result() {
    return result;
  }

  public int size() {
    return size;
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (ResultAndTotalSize) obj;
    return Objects.equals(this.result, that.result) && this.size == that.size;
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(result, size);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return "ResultAndTotalSize[" + "result=" + result + ", " + "size=" + size + ']';
  }
}
