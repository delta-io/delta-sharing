package io.whitefox.core.types;

import static io.whitefox.DeltaTestUtils.deltaTable;
import static io.whitefox.DeltaTestUtils.deltaTableUri;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;
import io.whitefox.core.PredicateUtils;
import io.whitefox.core.SharedTable;
import io.whitefox.core.types.predicates.*;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

public class PredicateUtilsTest {

  @Test
  @DisabledOnOs(OS.WINDOWS)
  void testCreateEvalContext() throws PredicateParsingException {
    var PTable = new SharedTable(
        "partitioned-delta-table-with-multiple-columns",
        "default",
        "share1",
        deltaTable("partitioned-delta-table-with-multiple-columns"));

    var log = DeltaLog.forTable(
        new Configuration(), deltaTableUri("partitioned-delta-table-with-multiple-columns"));
    var contexts = new ArrayList<EvalContext>();
    for (AddFile file : log.snapshot().getAllFiles()) {
      EvalContext evalContext = PredicateUtils.createEvalContext(file);
      contexts.add(evalContext);
    }
    assert (contexts.size() == 2);
    var c1 = contexts.get(0);
    var c2 = contexts.get(1);
    assert (c1.getPartitionValues().get("date").equals("2022-02-06"));
    assert (c2.getPartitionValues().get("date").equals("2021-09-12"));
  }
}
