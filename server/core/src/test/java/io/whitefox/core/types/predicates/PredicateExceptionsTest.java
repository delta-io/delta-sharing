package io.whitefox.core.types.predicates;

import static io.whitefox.DeltaTestUtils.deltaTableUri;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;
import io.whitefox.core.PredicateUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

@DisabledOnOs(OS.WINDOWS)
public class PredicateExceptionsTest {

  DeltaLog log = DeltaLog.forTable(
      new Configuration(), deltaTableUri("partitioned-delta-table-with-multiple-columns"));
  AddFile file = log.snapshot().getAllFiles().get(0);

  @Test
  void testTypeNotSupportedExceptionGettingThrown() throws PredicateParsingException {

    EvalContext context = PredicateUtils.createEvalContext(file);
    var predicate = "{"
        + "      \"op\":\"equal\",\n"
        + "      \"children\":[\n"
        + "        {\"op\":\"column\",\"name\":\"dating\",\"valueType\":\"float\"},\n"
        + "        {\"op\":\"literal\",\"value\":\"2021-09-12\",\"valueType\":\"float\"}\n"
        + "      ]\n"
        + "}";

    var parsed = PredicateUtils.parseJsonPredicate(predicate);
    assertThrows(TypeNotSupportedException.class, () -> parsed.evalExpectBoolean(context));
  }

  @Test
  void testNonExistingColumnExceptionGettingThrown() throws PredicateParsingException {

    EvalContext context = PredicateUtils.createEvalContext(file);
    var predicate = "{"
        + "      \"op\":\"equal\",\n"
        + "      \"children\":[\n"
        + "        {\"op\":\"column\",\"name\":\"notPresent\",\"valueType\": \"date\"},\n"
        + "        {\"op\":\"literal\",\"value\":\"2021-09-12\",\"valueType\":\"date\"}\n"
        + "      ]\n"
        + "}";

    var parsed = PredicateUtils.parseJsonPredicate(predicate);
    assertThrows(NonExistingColumnException.class, () -> parsed.evalExpectBoolean(context));
  }

  @Test
  void testTypeMismatchExceptionGettingThrown() throws PredicateParsingException {

    EvalContext context = PredicateUtils.createEvalContext(file);
    var predicate = "{"
        + "      \"op\":\"equal\",\n"
        + "      \"children\":[\n"
        + "        {\"op\":\"column\",\"name\":\"id\",\"valueType\":\"int\"},\n"
        + "        {\"op\":\"literal\",\"value\":\"abcd\",\"valueType\":\"string\"}\n"
        + "      ]\n"
        + "}";

    var parsed = PredicateUtils.parseJsonPredicate(predicate);
    assertThrows(TypeMismatchException.class, () -> parsed.evalExpectBoolean(context));
  }

  @Test
  void testTypeValidationExceptionGettingThrown() throws PredicateParsingException {

    EvalContext context = PredicateUtils.createEvalContext(file);
    var predicate = "{"
        + "      \"op\":\"equal\",\n"
        + "      \"children\":[\n"
        + "        {\"op\":\"column\",\"name\":\"id\",\"valueType\":\"int\"},\n"
        + "        {\"op\":\"literal\",\"value\":\"abcd\",\"valueType\":\"int\"}\n"
        + "      ]\n"
        + "}";

    var parsed = PredicateUtils.parseJsonPredicate(predicate);
    assertThrows(TypeValidationException.class, () -> parsed.evalExpectBoolean(context));
  }
}
