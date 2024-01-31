package io.whitefox.core.services;

import static io.whitefox.DeltaTestUtils.deltaTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.wildfly.common.Assert.assertTrue;

import io.whitefox.core.Protocol;
import io.whitefox.core.ReadTableRequest;
import io.whitefox.core.SharedTable;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

@DisabledOnOs(OS.WINDOWS)
public class DeltaSharedTableTest {

  @Test
  void getTableVersion() {
    var PTable = new SharedTable("delta-table", "default", "share1", deltaTable("delta-table"));
    var DTable = DeltaSharedTable.of(PTable);
    var version = DTable.getTableVersion(Optional.empty());
    assertEquals(Optional.of(0L), version);
  }

  @Test
  void getTableMetadata() {
    var PTable = new SharedTable("delta-table", "default", "share1", deltaTable("delta-table"));
    var DTable = DeltaSharedTable.of(PTable);
    var metadata = DTable.getMetadata(Optional.empty());
    assertTrue(metadata.isPresent());
    assertEquals("56d48189-cdbc-44f2-9b0e-2bded4c79ed7", metadata.get().id());
  }

  @Test
  void getUnknownTableMetadata() {
    var unknownPTable = new SharedTable("notFound", "default", "share1", deltaTable("location1"));
    assertThrows(IllegalArgumentException.class, () -> DeltaSharedTable.of(unknownPTable));
  }

  @Test
  void getTableVersionNonExistingTable() {
    var PTable =
        new SharedTable("delta-table", "default", "share1", deltaTable("delta-table-not-exists"));
    var exception = assertThrows(IllegalArgumentException.class, () -> DeltaSharedTable.of(PTable));
    assertTrue(exception.getMessage().startsWith("Cannot find a delta table at file"));
  }

  @Test
  void getTableVersionWithTimestamp() {
    var PTable = new SharedTable("delta-table", "default", "share1", deltaTable("delta-table"));
    var DTable = DeltaSharedTable.of(PTable);
    var version = DTable.getTableVersion(TestDateUtils.parseTimestamp("2023-09-30T10:15:30+01:00"));
    assertEquals(Optional.empty(), version);
  }

  @Test
  void getTableVersionWithFutureTimestamp() {
    var PTable = new SharedTable("delta-table", "default", "share1", deltaTable("delta-table"));
    var DTable = DeltaSharedTable.of(PTable);
    var version = DTable.getTableVersion(TestDateUtils.parseTimestamp("2024-10-20T10:15:30+01:00"));
    assertEquals(Optional.empty(), version);
  }

  @Test
  void getTableVersionWithMalformedTimestamp() {
    var PTable = new SharedTable("delta-table", "default", "share1", deltaTable("delta-table"));
    var DTable = DeltaSharedTable.of(PTable);
    assertThrows(
        DateTimeParseException.class,
        () -> DTable.getTableVersion(TestDateUtils.parseTimestamp("221rfewdsad10:15:30+01:00")));
  }

  @Test
  void queryTableWithoutPredicate() {
    var PTable = new SharedTable(
        "partitioned-delta-table", "default", "share1", deltaTable("partitioned-delta-table"));
    var DTable = DeltaSharedTable.of(PTable);
    var request = new ReadTableRequest.ReadTableCurrentVersion(
        Optional.empty(), Optional.empty(), Optional.empty());
    var response = DTable.queryTable(request);
    assertEquals(response.protocol(), new Protocol(Optional.of(1)));
    assertEquals(response.other().size(), 9);
  }

  @Test
  void queryTableWithSqlPredicates() {

    var predicatesAndExpectedResult = List.of(
        Pair.of(List.of("date = '2021-08-15'"), 4),
        Pair.of(List.of("date < '2021-08-14'"), 5),
        Pair.of(List.of("date > '2021-08-04'"), 9),
        Pair.of(List.of("date is NULL"), 0),
        Pair.of(List.of("date >= '2021-08-15'"), 4),
        Pair.of(List.of("date <> '2021-08-15'"), 5),
        Pair.of(List.of("date <= '2021-08-15'"), 9));

    var PTable = new SharedTable(
        "partitioned-delta-table", "default", "share1", deltaTable("partitioned-delta-table"));
    var DTable = DeltaSharedTable.of(PTable);

    predicatesAndExpectedResult.forEach(p -> {
      var request = new ReadTableRequest.ReadTableCurrentVersion(
          Optional.of(p.getLeft()), Optional.empty(), Optional.empty());
      var response = DTable.queryTable(request);
      assertEquals(p.getRight(), response.other().size());
    });
  }

  @Test
  void queryTableWithNonPartitionSqlPredicate() {
    var predicates = List.of("id < 30");
    var tableName = "partitioned-delta-table-with-multiple-columns";

    var PTable = new SharedTable(tableName, "default", "share1", deltaTable(tableName));
    var DTable = DeltaSharedTable.of(PTable);
    var request = new ReadTableRequest.ReadTableCurrentVersion(
        Optional.of(predicates), Optional.empty(), Optional.empty());
    var response = DTable.queryTable(request);
    assertEquals(1, response.other().size());
  }

  @Test
  void queryTableWithJsonPredicates() {

    var predicatesAndExpectedResult = List.of(
        Pair.of(
            "{"
                + "      \"op\":\"equal\",\n"
                + "      \"children\":[\n"
                + "        {\"op\":\"column\",\"name\":\"dating\",\"valueType\":\"date\"},\n"
                + "        {\"op\":\"literal\",\"value\":\"2021-09-12\",\"valueType\":\"date\"}\n"
                + "      ]\n"
                + "}",
            2),
        Pair.of(
            "{"
                + "      \"op\":\"equal\",\n"
                + "      \"children\":[\n"
                + "        {\"op\":\"column\",\"name\":\"date\",\"valueType\":\"date\"},\n"
                + "        {\"op\":\"literal\",\"value\":\"2021-09-12\",\"valueType\":\"date\"}\n"
                + "      ]\n"
                + "}",
            1),
        Pair.of(
            "{"
                + "      \"op\":\"lessThan\",\n"
                + "      \"children\":[\n"
                + "        {\"op\":\"column\",\"name\":\"id\",\"valueType\":\"int\"},\n"
                + "        {\"op\":\"literal\",\"value\":\"31\",\"valueType\":\"int\"}\n"
                + "      ]\n"
                + "}",
            1),
        Pair.of(
            "{"
                + "      \"op\":\"lessThanOrEqual\",\n"
                + "      \"children\":[\n"
                + "        {\"op\":\"column\",\"name\":\"id\",\"valueType\":\"int\"},\n"
                + "        {\"op\":\"literal\",\"value\":\"31\",\"valueType\":\"int\"}\n"
                + "      ]\n"
                + "}",
            2),
        Pair.of(
            "{"
                + "      \"op\":\"lessThan\",\n"
                + "      \"children\":[\n"
                + "        {\"op\":\"column\",\"name\":\"id\",\"valueType\":\"int\"},\n"
                + "        {\"op\":\"literal\",\"value\":\"45\",\"valueType\":\"int\"}\n"
                + "      ]\n"
                + "}",
            2),
        Pair.of(
            "{"
                + "      \"op\":\"isNull\",\n"
                + "      \"children\":[\n"
                + "        {\"op\":\"column\",\"name\":\"id\",\"valueType\":\"int\"}"
                + "      ]\n"
                + "}",
            2),
        Pair.of(
            "{\n" + "  \"op\":\"and\",\n"
                + "  \"children\":[\n"
                + "    {\n"
                + "      \"op\":\"equal\",\n"
                + "      \"children\":[\n"
                + "        {\"op\":\"column\",\"name\":\"date\",\"valueType\":\"date\"},\n"
                + "        {\"op\":\"literal\",\"value\":\"2022-02-06\",\"valueType\":\"date\"}\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"op\":\"greaterThanOrEqual\",\"children\":[\n"
                + "        {\"op\":\"column\",\"name\":\"id\",\"valueType\":\"int\"},\n"
                + "        {\"op\":\"literal\",\"value\":\"31\",\"valueType\":\"int\"}\n"
                + "      ]\n"
                + "    }\n"
                + "  ]\n"
                + "}",
            0));

    var PTable = new SharedTable(
        "partitioned-delta-table-with-multiple-columns",
        "default",
        "share1",
        deltaTable("partitioned-delta-table-with-multiple-columns"));
    var DTable = DeltaSharedTable.of(PTable);

    predicatesAndExpectedResult.forEach(p -> {
      var request = new ReadTableRequest.ReadTableCurrentVersion(
          Optional.empty(), Optional.of(p.getLeft()), Optional.empty());
      var response = DTable.queryTable(request);
      assertEquals(p.getRight(), response.other().size());
    });
  }
}
