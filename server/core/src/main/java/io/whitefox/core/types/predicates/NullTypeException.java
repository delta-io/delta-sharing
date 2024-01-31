package io.whitefox.core.types.predicates;

public class NullTypeException extends PredicateException {

  private final BaseOp leftChild;
  private final BaseOp rightChild;

  public NullTypeException(BaseOp leftChild, BaseOp rightChild) {
    this.leftChild = leftChild;
    this.rightChild = rightChild;
  }

  @Override
  public String getMessage() {
    // TODO: Currently means that the column is not in the partition columns
    // Expected is to mean that it is not present at all(use file stats)
    return "Comparison with a null value is not supported: " + leftChild.getClass() + " and "
        + rightChild.getClass();
  }
}
