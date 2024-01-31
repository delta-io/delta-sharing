package io.whitefox.core;

import io.whitefox.annotations.SkipCoverageGenerated;
import io.whitefox.core.types.*;
import io.whitefox.core.types.predicates.TypeNotSupportedException;
import java.sql.Date;
import java.sql.Timestamp;

public class ColumnRange {

  String minVal;
  String maxVal;

  DataType valueType;

  public ColumnRange(String minVal, String maxVal, DataType valueType) {
    this.minVal = minVal;
    this.maxVal = maxVal;
    this.valueType = valueType;
  }

  public ColumnRange(String onlyVal, DataType valueType) {
    this.minVal = onlyVal;
    this.maxVal = onlyVal;
    this.valueType = valueType;
  }

  public DataType getValueType() {
    return valueType;
  }

  public String getSingleValue() {
    return minVal;
  }

  private Boolean typedContains(String point) throws TypeNotSupportedException {
    if (valueType instanceof IntegerType) {
      var c1 = Integer.compare(Integer.parseInt(minVal), Integer.parseInt(point));
      var c2 = Integer.compare(Integer.parseInt(maxVal), Integer.parseInt(point));
      return (c1 <= 0 && c2 >= 0);
    } else if (valueType instanceof LongType) {
      var c1 = Long.compare(Long.parseLong(minVal), Long.parseLong(point));
      var c2 = Long.compare(Long.parseLong(maxVal), Long.parseLong(point));
      return (c1 <= 0 && c2 >= 0);
    } else if (valueType instanceof TimestampType) {
      var c1 = Timestamp.valueOf(minVal).before(Timestamp.valueOf(point));
      var c2 = Timestamp.valueOf(maxVal).after(Timestamp.valueOf(point));
      return (c1 && c2) || Timestamp.valueOf(minVal).equals(Timestamp.valueOf(point));
    } else if (valueType instanceof FloatType) {
      var c1 = Float.compare(Float.parseFloat(minVal), Float.parseFloat(point));
      var c2 = Float.compare(Float.parseFloat(maxVal), Float.parseFloat(point));
      return (c1 <= 0 && c2 >= 0);
    } else if (valueType instanceof DoubleType) {
      var c1 = Double.compare(Double.parseDouble(minVal), Double.parseDouble(point));
      var c2 = Double.compare(Double.parseDouble(maxVal), Double.parseDouble(point));
      return (c1 <= 0 && c2 >= 0);
    } else if (valueType instanceof DateType) {
      var c1 = Date.valueOf(minVal).before(Date.valueOf(point));
      var c2 = Date.valueOf(maxVal).after(Date.valueOf(point));
      return (c1 && c2) || Date.valueOf(minVal).equals(Date.valueOf(point));
    } else if (valueType instanceof BooleanType) {
      var c1 = Boolean.parseBoolean(minVal) == Boolean.parseBoolean(point);
      var c2 = Boolean.parseBoolean(maxVal) == Boolean.parseBoolean(point);
      return c1 || c2;
    } else if (valueType instanceof StringType) {
      var c1 = minVal.compareTo(point);
      var c2 = maxVal.compareTo(point);
      return (c1 <= 0 && c2 >= 0);
    } else throw new TypeNotSupportedException(valueType);
  }

  private Boolean typedLessThan(String point) throws TypeNotSupportedException {
    if (valueType instanceof IntegerType) {
      var c1 = Integer.compare(Integer.parseInt(minVal), Integer.parseInt(point));
      return (c1 < 0);
    } else if (valueType instanceof LongType) {
      var c1 = Long.compare(Long.parseLong(minVal), Long.parseLong(point));
      return (c1 < 0);
    } else if (valueType instanceof TimestampType) {
      return Timestamp.valueOf(minVal).before(Timestamp.valueOf(point));
    } else if (valueType instanceof FloatType) {
      var c1 = Float.compare(Float.parseFloat(minVal), Float.parseFloat(point));
      return (c1 < 0);
    } else if (valueType instanceof DoubleType) {
      var c1 = Double.compare(Double.parseDouble(minVal), Double.parseDouble(point));
      return (c1 < 0);
    } else if (valueType instanceof DateType) {
      return Date.valueOf(minVal).before(Date.valueOf(point));
    } else if (valueType instanceof StringType) {
      var c = minVal.compareTo(point);
      return (c < 0);
    } else throw new TypeNotSupportedException(valueType);
  }

  // not used currently
  @SkipCoverageGenerated
  private Boolean typedGreaterThan(String point) {
    if (valueType instanceof IntegerType) {
      var c = Integer.compare(Integer.parseInt(point), Integer.parseInt(maxVal));
      return (c < 0);
    } else if (valueType instanceof LongType) {
      var c = Long.compare(Long.parseLong(point), Long.parseLong(maxVal));
      return (c < 0);
    } else if (valueType instanceof TimestampType) {
      return Timestamp.valueOf(point).before(Timestamp.valueOf(maxVal));
    } else if (valueType instanceof FloatType) {
      var c = Float.compare(Float.parseFloat(maxVal), Float.parseFloat(point));
      return (c < 0);
    } else if (valueType instanceof DoubleType) {
      var c = Double.compare(Double.parseDouble(maxVal), Double.parseDouble(point));
      return (c < 0);
    } else if (valueType instanceof DateType) {
      return Date.valueOf(point).before(Date.valueOf(maxVal));

    } else {
      var c = point.compareTo(maxVal);
      return (c < 0);
    }
  }

  public Boolean contains(String point) throws TypeNotSupportedException {
    return typedContains(point);
  }

  public Boolean canBeLess(String point) throws TypeNotSupportedException {
    return typedLessThan(point);
  }

  public Boolean canBeGreater(String point) {
    return typedGreaterThan(point);
  }
}
