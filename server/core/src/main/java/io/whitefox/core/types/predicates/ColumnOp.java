package io.whitefox.core.types.predicates;

import static io.whitefox.core.PredicateUtils.createColumnRange;
import static io.whitefox.core.types.predicates.EvaluatorVersion.V1;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.whitefox.core.ColumnRange;
import io.whitefox.core.types.BooleanType;
import io.whitefox.core.types.DataType;
import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "column")
public class ColumnOp extends LeafOp {

  @JsonProperty("name")
  String name;

  public ColumnOp() {
    super();
  }

  public ColumnOp(String name, DataType valueType) {
    this.name = name;
    this.valueType = valueType;
  }

  // Determine if the column value is null.
  @Override
  public Boolean isNull(EvalContext ctx) {
    return ctx.partitionValues.get(name) == null && !ctx.getStatsValues().containsKey(name);
  }

  @Override
  public Boolean evalExpectBoolean(EvalContext ctx) {
    if (!Objects.equals(valueType, BooleanType.BOOLEAN)) {
      throw new IllegalArgumentException("Unsupported type for boolean evaluation: " + valueType);
    }
    return Boolean.valueOf(resolve(ctx));
  }

  public ColumnRange evalExpectColumnRange(EvalContext ctx) throws NonExistingColumnException {
    return createColumnRange(name, ctx, valueType);
  }

  @Override
  public DataType getOpValueType() {
    return valueType;
  }

  @Override
  public Object eval(EvalContext ctx) {
    // TODO: handle case of null column + column ranges
    return new ColumnRange(resolve(ctx), valueType);
  }

  public void validate() throws PredicateException {
    if (name == null) {
      throw new NonExistingColumnException("Name must be specified: " + this);
    }
    if (!this.isSupportedType(valueType, V1)) {
      throw new TypeNotSupportedException(valueType);
    }
  }

  private String resolve(EvalContext ctx) {
    // TODO: handle case of null column + column ranges
    return ctx.partitionValues.getOrDefault(name, null);
  }
}
