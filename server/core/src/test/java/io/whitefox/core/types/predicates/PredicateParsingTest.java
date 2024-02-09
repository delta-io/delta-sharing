package io.whitefox.core.types.predicates;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.whitefox.core.Metadata;
import io.whitefox.core.PredicateUtils;
import io.whitefox.core.TableSchema;
import io.whitefox.core.services.capabilities.ResponseFormat;
import io.whitefox.core.types.DateType;
import io.whitefox.core.types.StructField;
import io.whitefox.core.types.StructType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class PredicateParsingTest {

  @Test
  void testParsingOfJsonEqual() throws PredicateException {

    String predicate = "{\n" + "  \"op\": \"equal\",\n"
        + "  \"children\": [\n"
        + "    {\"op\": \"column\", \"name\":\"hireDate\", \"valueType\":\"date\"},\n"
        + "    {\"op\":\"literal\",\"value\":\"2021-04-29\",\"valueType\":\"date\"}\n"
        + "  ]\n"
        + "}";
    var op = PredicateUtils.parseJsonPredicate(predicate);
    op.validate();
    assert (op instanceof EqualOp);
    assert (((EqualOp) op).children.size() == 2);
  }

  @Test
  void testParsingOfInvalidSql() {
    var meta = new Metadata(
        "id",
        Optional.empty(),
        Optional.empty(),
        ResponseFormat.parquet,
        new TableSchema(
            new StructType(List.of(new StructField("date", DateType.DATE, true, Map.of())))),
        List.of("date", "age"),
        Map.of(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());

    var ctx = new EvalContext(Map.of("date", "date"), Map.of());
    var predicate = "date LIKE '2021-09-09'";

    assertThrows(
        ExpressionNotSupportedException.class,
        () -> PredicateUtils.parseSqlPredicate(predicate, ctx, meta));
  }

  @Test
  void testParsingOfSqlEqual() throws PredicateException {
    var ctx = new EvalContext(Map.of("date", "date"), Map.of());
    var meta = new Metadata(
        "id",
        Optional.empty(),
        Optional.empty(),
        ResponseFormat.parquet,
        new TableSchema(
            new StructType(List.of(new StructField("date", DateType.DATE, true, Map.of())))),
        List.of("date", "age"),
        Map.of(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
    var predicate = "date = '2021-09-09'";
    var op = PredicateUtils.parseSqlPredicate(predicate, ctx, meta);
    op.validate();
    assert (op instanceof EqualOp);
    assert (((EqualOp) op).children.size() == 2);
  }

  @Test
  void testParsingOfNested() throws PredicateException {
    String predicate = "{\n" + "  \"op\":\"and\",\n"
        + "  \"children\":[\n"
        + "    {\n"
        + "      \"op\":\"equal\",\n"
        + "      \"children\":[\n"
        + "        {\"op\":\"column\",\"name\":\"hireDate\",\"valueType\":\"date\"},\n"
        + "        {\"op\":\"literal\",\"value\":\"2021-04-29\",\"valueType\":\"date\"}\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"op\":\"lessThan\",\"children\":[\n"
        + "        {\"op\":\"column\",\"name\":\"id\",\"valueType\":\"int\"},\n"
        + "        {\"op\":\"literal\",\"value\":\"25\",\"valueType\":\"int\"}\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    var op = PredicateUtils.parseJsonPredicate(predicate);
    op.validate();
    assert (op instanceof AndOp);
    assert (((AndOp) op).children.size() == 2);
    assert (((AndOp) op).children.get(0) instanceof EqualOp);
  }

  @Test
  void testCustomExceptionOnBadJson() {
    String predicate = "{\n" + "  \"op\":\"and\",\n"
        + "  \"children\":[\n"
        + "    {\n"
        + "      \"op\":\"equals\",\n"
        + "      \"children\":[\n"
        + "        {\"op\":\"columna\",\"name\":\"hireDate\",\"valueType\":\"date\"},\n"
        + "        {\"op\":\"literal\",\"value\":\"2021-04-29\",\"valueType\":\"date\"}\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"op\":\"lessThans\",\"children\":[\n"
        + "        {\"op\":\"column\",\"name\":\"id\",\"valueType\":\"int\"},\n"
        + "        {\"op\":\"literal\",\"value\":\"25\",\"valueType\":\"int\"}\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    assertThrows(
        PredicateParsingException.class, () -> PredicateUtils.parseJsonPredicate(predicate));
  }
}
