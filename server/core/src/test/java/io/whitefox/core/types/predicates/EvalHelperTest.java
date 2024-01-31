package io.whitefox.core.types.predicates;

import static org.junit.jupiter.api.Assertions.*;

import io.whitefox.core.types.*;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

public class EvalHelperTest {

  @Test
  void testLessThanOnPartitionColumns() throws PredicateException {
    var evalContext1 = new EvalContext(Map.of("date", "2020-10-10"), Map.of());
    var children1 =
        List.of(new ColumnOp("date", DateType.DATE), new LiteralOp("2020-10-11", DateType.DATE));
    assertTrue(EvalHelper.lessThan(children1, evalContext1));
    var evalContext2 = new EvalContext(Map.of("integerCol", "19"), Map.of());
    var children2 = List.of(
        new ColumnOp("integerCol", IntegerType.INTEGER), new LiteralOp("20", IntegerType.INTEGER));
    assertTrue(EvalHelper.lessThan(children2, evalContext2));
    var evalContext3 = new EvalContext(Map.of("long", "20"), Map.of());
    var children3 =
        List.of(new ColumnOp("long", LongType.LONG), new LiteralOp("21", LongType.LONG));
    assertTrue(EvalHelper.lessThan(children3, evalContext3));
    var evalContext4 = new EvalContext(Map.of("float", "2.97"), Map.of());
    var children4 =
        List.of(new ColumnOp("float", FloatType.FLOAT), new LiteralOp("2.98", FloatType.FLOAT));
    assertTrue(EvalHelper.lessThan(children4, evalContext4));
    var children5 =
        List.of(new LiteralOp("21", LongType.LONG), new ColumnOp("long", LongType.LONG));
    assertTrue(EvalHelper.lessThan(children5, evalContext3));
    var evalContext6 = new EvalContext(Map.of("float", "2.99"), Map.of());
    var children6 =
        List.of(new ColumnOp("float", FloatType.FLOAT), new LiteralOp("2.98", FloatType.FLOAT));
    assertFalse(EvalHelper.lessThan(children6, evalContext6));
  }

  @Test
  void testEqualOnPartitionColumns() throws PredicateException {
    var evalContext1 = new EvalContext(Map.of("date", "2020-10-11"), Map.of());
    var children1 =
        List.of(new ColumnOp("date", DateType.DATE), new LiteralOp("2020-10-11", DateType.DATE));
    assertTrue(EvalHelper.equal(children1, evalContext1));
    var evalContext2 = new EvalContext(Map.of("integerCol", "20"), Map.of());
    var children2 = List.of(
        new ColumnOp("integerCol", IntegerType.INTEGER), new LiteralOp("20", IntegerType.INTEGER));
    assertTrue(EvalHelper.equal(children2, evalContext2));
    var evalContext3 = new EvalContext(Map.of("long", "20"), Map.of());
    var children3 =
        List.of(new ColumnOp("long", LongType.LONG), new LiteralOp("20", LongType.LONG));
    assertTrue(EvalHelper.equal(children3, evalContext3));
    var evalContext4 = new EvalContext(Map.of("boolean", "true"), Map.of());
    var children4 = List.of(
        new ColumnOp("boolean", BooleanType.BOOLEAN), new LiteralOp("true", BooleanType.BOOLEAN));
    assertTrue(EvalHelper.equal(children4, evalContext4));
    var evalContext5 = new EvalContext(Map.of("float", "2.99"), Map.of());
    var children5 =
        List.of(new ColumnOp("float", FloatType.FLOAT), new LiteralOp("2.99", FloatType.FLOAT));
    assertTrue(EvalHelper.equal(children5, evalContext5));
  }

  @Test
  void testEqualOnRegularColumns() throws PredicateException {
    var evalContext1 = new EvalContext(
        Map.of("date", "2020-10-11"), Map.of("id", Pair.of("2020-11-10", "2020-11-12")));
    var children1 =
        List.of(new ColumnOp("id", DateType.DATE), new LiteralOp("2020-11-11", DateType.DATE));
    assertTrue(EvalHelper.equal(children1, evalContext1));
    var evalContext2 = new EvalContext(Map.of("integer", "20"), Map.of("id", Pair.of("20", "29")));
    var children2 =
        List.of(new ColumnOp("id", IntegerType.INTEGER), new LiteralOp("20", IntegerType.INTEGER));
    assertTrue(EvalHelper.equal(children2, evalContext2));
    var evalContext3 = new EvalContext(Map.of("long", "20"), Map.of("id", Pair.of("20", "29")));
    var children3 = List.of(new ColumnOp("id", LongType.LONG), new LiteralOp("21", LongType.LONG));
    assertTrue(EvalHelper.equal(children3, evalContext3));
    var evalContext4 = new EvalContext(Map.of("long", "20"), Map.of("id", Pair.of("2.99", "3.01")));
    var children4 =
        List.of(new ColumnOp("id", FloatType.FLOAT), new LiteralOp("3.0", FloatType.FLOAT));
    assertTrue(EvalHelper.equal(children4, evalContext4));
    var evalContext5 = new EvalContext(Map.of("long", "20"), Map.of("id", Pair.of("aaaa", "cccc")));
    var children5 =
        List.of(new ColumnOp("id", StringType.STRING), new LiteralOp("aabb", StringType.STRING));
    assertTrue(EvalHelper.equal(children5, evalContext5));
    var evalContext6 = new EvalContext(Map.of("long", "20"), Map.of("id", Pair.of("2.99", "3.01")));
    var children6 =
        List.of(new ColumnOp("id", DoubleType.DOUBLE), new LiteralOp("3.0", DoubleType.DOUBLE));
    assertTrue(EvalHelper.equal(children6, evalContext6));
    var evalContext7 = new EvalContext(Map.of("long", "20"), Map.of("id", Pair.of("true", "true")));
    var children7 = List.of(
        new ColumnOp("id", BooleanType.BOOLEAN), new LiteralOp("true", BooleanType.BOOLEAN));
    assertTrue(EvalHelper.equal(children7, evalContext7));
    var evalContext8 = new EvalContext(
        Map.of("long", "20"),
        Map.of("id", Pair.of("2022-08-10 06:02:03.000000", "2022-12-10 06:02:03.000000")));
    var children8 = List.of(
        new ColumnOp("id", TimestampType.TIMESTAMP),
        new LiteralOp("2022-10-10 06:02:03.000000", TimestampType.TIMESTAMP));
    assertTrue(EvalHelper.equal(children8, evalContext8));
  }

  @Test
  void testLessThanOnRegularColumns() throws PredicateException {
    var evalContext1 = new EvalContext(
        Map.of("date", "2020-10-11"), Map.of("id", Pair.of("2020-11-10", "2020-11-12")));
    var children1 =
        List.of(new ColumnOp("id", DateType.DATE), new LiteralOp("2020-12-12", DateType.DATE));
    assertTrue(EvalHelper.lessThan(children1, evalContext1));
    var evalContext2 = new EvalContext(Map.of("integer", "20"), Map.of("id", Pair.of("20", "29")));
    var children2 =
        List.of(new ColumnOp("id", IntegerType.INTEGER), new LiteralOp("30", IntegerType.INTEGER));
    assertTrue(EvalHelper.lessThan(children2, evalContext2));
    var evalContext3 = new EvalContext(Map.of("long", "20"), Map.of("id", Pair.of("20", "29")));
    var children3 = List.of(new ColumnOp("id", LongType.LONG), new LiteralOp("30", LongType.LONG));
    assertTrue(EvalHelper.lessThan(children3, evalContext3));
    var evalContext4 = new EvalContext(Map.of("long", "20"), Map.of("id", Pair.of("2.99", "3.01")));
    var children4 =
        List.of(new ColumnOp("id", FloatType.FLOAT), new LiteralOp("3.02", FloatType.FLOAT));
    assertTrue(EvalHelper.lessThan(children4, evalContext4));
    var evalContext5 = new EvalContext(Map.of("long", "20"), Map.of("id", Pair.of("2.99", "3.01")));
    var children5 =
        List.of(new ColumnOp("id", DoubleType.DOUBLE), new LiteralOp("3.02", DoubleType.DOUBLE));
    assertTrue(EvalHelper.lessThan(children5, evalContext5));
    var evalContext6 = new EvalContext(Map.of("long", "20"), Map.of("id", Pair.of("2.99", "3.01")));
    var children6 =
        List.of(new ColumnOp("id", DoubleType.DOUBLE), new LiteralOp("3.02", DoubleType.DOUBLE));
    assertTrue(EvalHelper.lessThan(children6, evalContext6));
    var evalContext7 = new EvalContext(Map.of("long", "20"), Map.of("id", Pair.of("aaaa", "cccc")));
    var children7 =
        List.of(new ColumnOp("id", StringType.STRING), new LiteralOp("dddd", StringType.STRING));
    assertTrue(EvalHelper.lessThan(children7, evalContext7));
    var evalContext8 = new EvalContext(
        Map.of("long", "20"),
        Map.of("id", Pair.of("2022-08-10 06:02:03.000000", "2022-12-10 06:02:03.000000")));
    var children8 = List.of(
        new ColumnOp("id", TimestampType.TIMESTAMP),
        new LiteralOp("2022-12-11 06:02:03.000000", TimestampType.TIMESTAMP));
    assertTrue(EvalHelper.lessThan(children8, evalContext8));
  }

  @Test
  void testValidateAndGetRange() throws PredicateException {
    var evalContext1 =
        new EvalContext(Map.of("date", "2020-10-11"), Map.of("id", Pair.of("20", "29")));
    var children1 = List.of(
        new ColumnOp("notId", IntegerType.INTEGER), new LiteralOp("30", IntegerType.INTEGER));
    assertThrows(
        NonExistingColumnException.class, () -> EvalHelper.lessThan(children1, evalContext1));
    var children2 =
        List.of(new ColumnOp("id", IntegerType.INTEGER), new LiteralOp("30", IntegerType.INTEGER));
    assertTrue(EvalHelper.lessThan(children2, evalContext1));
    assertFalse(EvalHelper.equal(children2, evalContext1));
    var reversedChildren =
        List.of(new LiteralOp("30", IntegerType.INTEGER), new ColumnOp("id", IntegerType.INTEGER));
    assertFalse(EvalHelper.equal(reversedChildren, evalContext1));
    var evalContext2 =
        new EvalContext(Map.of("date", "2020-10-11"), Map.of("id", Pair.of("20", "29")));
    var children3 =
        List.of(new ColumnOp("notId", IntegerType.INTEGER), new LiteralOp("30", DateType.DATE));
    assertThrows(TypeMismatchException.class, () -> EvalHelper.lessThan(children3, evalContext2));
    assertThrows(TypeMismatchException.class, () -> EvalHelper.equal(children3, evalContext2));
  }
}
