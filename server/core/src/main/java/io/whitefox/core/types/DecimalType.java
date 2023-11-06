package io.whitefox.core.types;

import io.whitefox.annotations.SkipCoverageGenerated;
import java.util.Objects;

public final class DecimalType extends DataType {
  public static final DecimalType USER_DEFAULT = new DecimalType(10, 0);

  private final int precision;
  private final int scale;

  public DecimalType(int precision, int scale) {
    if (precision < 0 || precision > 38 || scale < 0 || scale > 38 || scale > precision) {
      throw new IllegalArgumentException(String.format(
          "Invalid precision and scale combo (%d, %d). They should be in the range [0, 38] "
              + "and scale can not be more than the precision.",
          precision, scale));
    }
    this.precision = precision;
    this.scale = scale;
  }

  /**
   * @return the maximum number of digits of the decimal
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * @return the number of digits on the right side of the decimal point (dot)
   */
  public int getScale() {
    return scale;
  }

  @Override
  public String toJson() {
    return String.format("\"decimal(%d, %d)\"", precision, scale);
  }

  @Override
  @SkipCoverageGenerated
  public String toString() {
    return String.format("Decimal(%d, %d)", precision, scale);
  }

  @Override
  @SkipCoverageGenerated
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DecimalType that = (DecimalType) o;
    return precision == that.precision && scale == that.scale;
  }

  @Override
  @SkipCoverageGenerated
  public int hashCode() {
    return Objects.hash(precision, scale);
  }
}
