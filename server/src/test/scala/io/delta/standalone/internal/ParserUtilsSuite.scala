package io.delta.standalone.internal

import io.delta.standalone.internal.actions.AddFile
import org.apache.spark.sql.types.StructType
import org.scalatest.FunSuite

class ParserUtilsSuite extends FunSuite {

  test("parse partition filter") {
    val partitionSchema = StructType.fromDDL("c2 INT")
    val addFiles = Seq(
      AddFile("foo1", Map("c2" -> "0"), 1, 1, true),
      AddFile("foo2", Map("c2" -> "1"), 1, 1, true),
    )
    val filtered = ParserUtils.evaluatePredicate(partitionSchema, "c2 = 0", addFiles)
    assert(addFiles.take(1) == filtered)
    val filtered2 = ParserUtils.evaluatePredicate(partitionSchema, "c2 = 1", addFiles)
    assert(addFiles.tail == filtered2)
  }
}
