package io.whitefox.core.types.predicates;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.whitefox.core.ColumnRange;
import io.whitefox.core.types.DataType;
import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "op")
@JsonSubTypes({
  @JsonSubTypes.Type(value = ColumnOp.class, name = "column"),
  @JsonSubTypes.Type(value = LiteralOp.class, name = "literal")
})
public abstract class LeafOp implements BaseOp {

  abstract Boolean isNull(EvalContext ctx);

  @JsonProperty("valueType")
  DataType valueType;

  ColumnRange evalExpectValueAndType(EvalContext ctx) throws PredicateException {
    var res = eval(ctx);
    if (res instanceof ColumnRange) {
      return (ColumnRange) res;
    } else {
      throw new WrongExpectedTypeException(res, ColumnRange.class);
    }
  }

  @Override
  public List<BaseOp> getAllChildren() {
    return List.of();
  }

  abstract DataType getOpValueType();
}
