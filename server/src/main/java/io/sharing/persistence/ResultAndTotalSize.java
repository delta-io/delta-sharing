package io.sharing.persistence;

public class ResultAndTotalSize<T> {
  public final T result;
  public final int size;

  public ResultAndTotalSize(T result, int size) {
    this.result = result;
    this.size = size;
  }
}
