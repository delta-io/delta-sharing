package io.whitefox.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.standalone.actions.AddFile;
import io.whitefox.core.types.DataType;
import io.whitefox.core.types.predicates.*;
import java.util.List;
import java.util.Optional;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

public class PredicateUtils {

  private static final Logger logger = Logger.getLogger(PredicateUtils.class);

  private static final ObjectMapper objectMapper = DeltaObjectMapper.getInstance();

  public static BaseOp parseJsonPredicate(String predicate) throws PredicateParsingException {
    try {
      return objectMapper.readValue(predicate, BaseOp.class);
    } catch (JsonProcessingException e) {
      throw new PredicateParsingException(e);
    }
  }

  public static boolean evaluateJsonPredicate(
      Optional<String> predicate, EvalContext ctx, AddFile f) {
    try {
      if (predicate.isEmpty()) return true;
      else {
        var parsedPredicate = PredicateUtils.parseJsonPredicate(predicate.get());
        return parsedPredicate.evalExpectBoolean(ctx);
      }
    } catch (PredicateException e) {
      logger.debug("Caught exception for predicate: " + predicate + " - " + e.getMessage());
      logger.info("File: " + f.getPath()
          + " will be used in processing due to failure in parsing or processing the predicate: "
          + predicate);
      return true;
    }
  }

  public static boolean evaluateSqlPredicate(
      String predicate, EvalContext ctx, AddFile f, Metadata metadata) {
    try {
      var parsedPredicate = PredicateUtils.parseSqlPredicate(predicate, ctx, metadata);
      return parsedPredicate.evalExpectBoolean(ctx);
    } catch (PredicateException e) {
      logger.debug("Caught exception for predicate: " + predicate + " - " + e.getMessage());
      logger.info("File: " + f.getPath()
          + " will be used in processing due to failure in parsing or processing the predicate: "
          + predicate);
      return true;
    }
  }

  public static BaseOp parseSqlPredicate(String predicate, EvalContext ctx, Metadata metadata)
      throws PredicateException {
    try {
      var expression = CCJSqlParserUtil.parseCondExpression(predicate);
      if (expression instanceof IsNullExpression) {
        var isNullExpression = (IsNullExpression) expression;
        String column =
            isNullExpression.getLeftExpression().getASTNode().jjtGetFirstToken().toString();
        var dataType = metadata.tableSchema().structType().get(column).getDataType();
        var colOp = new ColumnOp(column, dataType);
        var children = List.of((LeafOp) colOp);
        var operator = "isnull";
        return NonLeafOp.createPartitionFilter(children, operator);
      } else if (expression instanceof BinaryExpression) {
        BinaryExpression binaryExpression = (BinaryExpression) expression;
        String column = binaryExpression.getLeftExpression().toString();
        String operator = binaryExpression.getStringExpression();
        Expression value = binaryExpression.getRightExpression();
        if (value instanceof StringValue) {
          StringValue stringValue = (StringValue) value;
          var dataType = metadata.tableSchema().structType().get(column).getDataType();
          var colOp = new ColumnOp(column, dataType);
          var litOp = new LiteralOp(stringValue.getValue(), dataType);
          var children = List.of(colOp, litOp);
          return NonLeafOp.createPartitionFilter(children, operator);
        } else {
          var dataType = metadata.tableSchema().structType().get(column).getDataType();
          var colOp = new ColumnOp(column, dataType);
          var litOp = new LiteralOp(value.toString(), dataType);
          var children = List.of(colOp, litOp);
          return NonLeafOp.createPartitionFilter(children, operator);
        }
      } else throw new ExpressionNotSupportedException(predicate);
    } catch (JSQLParserException e) {
      throw new PredicateParsingException(e);
    }
  }

  public static ColumnRange createColumnRange(String name, EvalContext ctx, DataType valueType)
      throws NonExistingColumnException {
    var fileStats = ctx.getStatsValues();
    var values = Optional.ofNullable(fileStats.get(name))
        .orElseThrow(() -> new NonExistingColumnException(name));
    return new ColumnRange(values.getLeft(), values.getRight(), valueType);
  }

  public static EvalContext createEvalContext(AddFile file) throws PredicateParsingException {
    var statsString = file.getStats();
    var partitionValues = file.getPartitionValues();

    try {
      var fileStats = objectMapper.readValue(statsString, FileStats.class);
      var maxValues = fileStats.maxValues;
      var mappedMinMaxPairs = new java.util.HashMap<String, Pair<String, String>>();
      fileStats.getMinValues().forEach((minK, minV) -> {
        String maxV = maxValues.get(minK);
        Pair<String, String> minMaxPair = Pair.of(minV, maxV);
        mappedMinMaxPairs.put(minK, minMaxPair);
      });
      return new EvalContext(partitionValues, mappedMinMaxPairs);
    } catch (JsonProcessingException e) {
      // should never happen, depends on if the delta implementation changes
      throw new PredicateParsingException(e);
    }
  }
}
