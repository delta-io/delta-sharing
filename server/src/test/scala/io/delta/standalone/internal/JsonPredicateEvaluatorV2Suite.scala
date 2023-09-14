/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println

package io.delta.standalone.internal

import org.scalatest.FunSuite

import io.delta.sharing.server.util.JsonUtils

class JsonPredicateEvaluatorV2Suite extends FunSuite {

  // A wrapper around op evaluation in the specified context (data file).
  //
  // If expectException is true, we expect validation to fail.
  // However, the evaluator should still evaluate it correctly and produce
  // the desired expectedRawResult' since it supports partial evaluation.
  def evalTest(
      op: BaseOp,
      ctx: EvalContext,
      expectedRawResult: EvalResult,
      expectException: Boolean = false): Unit = {
    var unexpectedException: Option[Exception] = None
    try {
      op.validate(true)
      if (expectException) {
        unexpectedException = Some(new IllegalArgumentException("Expected exception for " + op))
      }
    } catch {
      case e: IllegalArgumentException =>
        unexpectedException = if (expectException) {
          None
        } else {
          Some(e)
        }
    }
    if (unexpectedException.isDefined) {
      throw unexpectedException.get
    }

    val eval = new JsonPredicateEvaluatorV2(op)
    // Call raw evaluation, which should match expected raw result.
    assert(eval.evalRaw(ctx) == expectedRawResult)

    // Boolean evaluation should treat unknowns as true, which implies that the data file
    // will not be skipped.
    val expectedResult = expectedRawResult match {
      case EvalResultFalse => false
      case _ => true
    }
    assert(eval.eval(ctx) == expectedResult)
  }

  test("LiteralOp test") {
    // Test direct evaluation of boolean literals (which don't require a context).
    var ctx = EvalContext(Map.empty)
    evalTest(LiteralOp("true", "bool"), ctx, EvalResultTrue)
    evalTest(LiteralOp("false", "bool"), ctx, EvalResultFalse)

    // Direct evaluation of non-boolean literals is not supported.
    evalTest(LiteralOp("foo", "string"), ctx, EvalResultUnknown)
    evalTest(LiteralOp("1", "int"), ctx, EvalResultUnknown)
    evalTest(LiteralOp("1.0", "float"), ctx, EvalResultUnknown)

    // Trigger validation errors on literals.
    evalTest(LiteralOp("foo", "bool"), ctx, EvalResultUnknown, true)
    evalTest(LiteralOp("foo", "unsupportedType"), ctx, EvalResultUnknown, true)

    // Not on top of boolean literals.
    evalTest(NotOp(Seq(LiteralOp("true", "bool"))), ctx, EvalResultFalse)
    evalTest(NotOp(Seq(LiteralOp("false", "bool"))), ctx, EvalResultTrue)

    // Test Literals as part of And.
    ctx = EvalContext(Map("date" -> "2023-05-23"))
    val eqOp = EqualOp(List(ColumnOp("date", "date"), LiteralOp("2023-05-23", "date")))
    evalTest(AndOp(Seq(eqOp, LiteralOp("true", "bool"))), ctx, EvalResultTrue)
    evalTest(AndOp(Seq(eqOp, LiteralOp("false", "bool"))), ctx, EvalResultFalse)
  }

  test("ColumnOp test") {
    // Test direct evaluation of boolean partition columns.
    evalTest(ColumnOp("A", "bool"), EvalContext(Map("A" -> "true")), EvalResultTrue)
    evalTest(ColumnOp("A", "bool"), EvalContext(Map("A" -> "false")), EvalResultFalse)
    evalTest(ColumnOp("A", "bool"), EvalContext(Map.empty), EvalResultUnknown)
    evalTest(ColumnOp("A", "bool"), EvalContext(Map("A" -> "foo")), EvalResultUnknown)

    // Test error cases for direct evaluation of partition columns.
    evalTest(ColumnOp("A", "int"), EvalContext(Map("A" -> "1")), EvalResultUnknown)
    evalTest(ColumnOp("1.0", "double"), EvalContext(Map("A" -> "1.0")), EvalResultUnknown)
    evalTest(ColumnOp("A", "unsupported"), EvalContext(Map("A" -> "1")), EvalResultUnknown, true)

    // Test direct evaluation of boolean non-partition columns.
    evalTest(
      ColumnOp("A", "bool"),
      EvalContext(Map.empty, Map("A" -> (("true", "true")))),
      EvalResultTrue
    )
    evalTest(
      ColumnOp("A", "bool"),
      EvalContext(Map.empty, Map("A" -> (("false", "true")))),
      EvalResultTrue
    )
    evalTest(
      ColumnOp("A", "bool"),
      EvalContext(Map.empty, Map("A" -> (("false", "false")))),
      EvalResultFalse
    )

    // Test error cases for direct evaluation of non-partition columns.
    evalTest(
      ColumnOp("A", "bool"),
      EvalContext(Map.empty, Map("A" -> (("X", "Y")))),
      EvalResultUnknown
    )
    evalTest(
      ColumnOp("A", "string"),
      EvalContext(Map.empty, Map("A" -> (("X", "Y")))),
      EvalResultUnknown
    )
  }

  test("EqualOp test") {
    // Happy path: Equality evaluates to true.
    Seq(
      EvalContext(Map("A" -> "2021-04-29")),
      EvalContext(Map.empty, Map("A" -> (("2021-04-27", "2021-04-29")))),
      EvalContext(Map.empty, Map("A" -> (("2021-04-29", "2021-04-29")))),
      EvalContext(Map.empty, Map("A" -> (("2021-04-29", "2021-04-30")))),
      EvalContext(Map.empty, Map("A" -> (("2021-04-27", "2021-05-01"))))
    ).foreach(ctx => {
      evalTest(
        EqualOp(Seq(ColumnOp("A", "date"), LiteralOp("2021-04-29", "date"))),
        ctx,
        EvalResultTrue
      )
    })

    // Happy path and error cases.
    Seq(
      EvalContext(Map("A" -> "2021-04-28")),
      EvalContext(Map("A" -> "2021-05-02")),
      EvalContext(Map.empty, Map("A" -> (("2021-04-27", "2021-04-28")))),
      EvalContext(Map.empty, Map("A" -> (("2021-04-30", "2021-05-28"))))
    ).foreach(ctx => {
      // Happy path: equality evaluates to false.
      evalTest(
        EqualOp(Seq(ColumnOp("A", "date"), LiteralOp("2021-04-29", "date"))),
        ctx,
        EvalResultFalse
      )

      // Equality evaluates to unknown in case of missing/incorrect column stats.
      // Some of these will trigger validation failures as well.
      evalTest(
        EqualOp(Seq(ColumnOp("A", "bool"), LiteralOp("true", "bool"))),
        ctx,
        EvalResultUnknown
      )
      evalTest(
        EqualOp(Seq(ColumnOp("A", "int"), LiteralOp("1", "int"), LiteralOp("1", "int"))),
        ctx,
        EvalResultUnknown,
        true
      )
      evalTest(
        EqualOp(Seq(ColumnOp("A", "date"), LiteralOp("29", "int"))),
        ctx,
        EvalResultUnknown,
        true
      )
    })

    // All column stats missing.
    evalTest(
      EqualOp(Seq(ColumnOp("A", "date"), LiteralOp("2021-04-29", "date"))),
      EvalContext(Map.empty),
      EvalResultUnknown
    )
  }

  test("LessThanOp test") {
    // Happy path: lessThan evaluates to true.
    Seq(
      EvalContext(Map("A" -> "2021-04-28")),
      EvalContext(Map.empty, Map("A" -> (("2021-01-21", "2021-04-28")))),
      EvalContext(Map.empty, Map("A" -> (("2021-04-27", "2021-04-27")))),
      EvalContext(Map.empty, Map("A" -> (("2021-04-26", "2021-04-29")))),
      EvalContext(Map.empty, Map("A" -> (("2021-04-28", "2021-05-01"))))
    ).foreach(ctx => {
      evalTest(
        LessThanOp(Seq(ColumnOp("A", "date"), LiteralOp("2021-04-29", "date"))),
        ctx,
        EvalResultTrue
      )
    })

    Seq(
      EvalContext(Map("A" -> "2021-04-29")),
      EvalContext(Map("A" -> "2021-05-02")),
      EvalContext(Map.empty, Map("A" -> (("2021-04-29", "2021-04-30")))),
      EvalContext(Map.empty, Map("A" -> (("2021-04-30", "2021-05-28"))))
    ).foreach(ctx => {
      // Happy path: lessThan evaluates to false.
      evalTest(
        LessThanOp(Seq(ColumnOp("A", "date"), LiteralOp("2021-04-29", "date"))),
        ctx,
        EvalResultFalse
      )

      // Evaluates to unknown in case of missing/incorrect column stats.
      // Some of these will trigger validation failures as well.
      evalTest(
        LessThanOp(Seq(ColumnOp("A", "bool"), LiteralOp("true", "bool"))),
        ctx,
        EvalResultUnknown
      )
      evalTest(
        LessThanOp(Seq(ColumnOp("A", "int"), LiteralOp("1", "int"), LiteralOp("2", "int"))),
        ctx,
        EvalResultUnknown,
        true
      )
      evalTest(
        LessThanOp(Seq(ColumnOp("A", "date"), LiteralOp("29", "int"))),
        ctx,
        EvalResultUnknown,
        true
      )
    })

    // All column stats missing.
    evalTest(
      LessThanOp(Seq(ColumnOp("A", "date"), LiteralOp("2021-04-29", "date"))),
      EvalContext(Map.empty),
      EvalResultUnknown
    )
  }

  test("LessThanOrEqualOp test") {
    // Happy path: lessThanOrEqual evaluates to true.
    Seq(
      EvalContext(Map("A" -> "5.2")),
      EvalContext(Map.empty, Map("A" -> (("0.0", "200.0")))),
      EvalContext(Map.empty, Map("A" -> (("0.0", "5.2")))),
      EvalContext(Map.empty, Map("A" -> (("5.2", "100")))),
      EvalContext(Map.empty, Map("A" -> (("4.1", "5.21"))))
    ).foreach(ctx => {
      evalTest(
        LessThanOrEqualOp(Seq(ColumnOp("A", "float"), LiteralOp("5.2", "float"))),
        ctx,
        EvalResultTrue
      )
    })

    Seq(
      EvalContext(Map("A" -> "5.21")),
      EvalContext(Map("A" -> "100.0")),
      EvalContext(Map.empty, Map("A" -> (("5.21", "10.0")))),
      EvalContext(Map.empty, Map("A" -> (("100.0", "200.0"))))
    ).foreach(ctx => {
      // Happy path: lessThanOrEqual evaluates to false.
      evalTest(
        LessThanOrEqualOp(Seq(ColumnOp("A", "double"), LiteralOp("5.2", "double"))),
        ctx,
        EvalResultFalse
      )

      // Evaluates to unknown in case of missing/incorrect column stats.
      // Some of these will trigger validation failures as well.
      evalTest(
        LessThanOrEqualOp(Seq(ColumnOp("A", "bool"), LiteralOp("true", "bool"))),
        ctx,
        EvalResultUnknown
      )
      evalTest(
        LessThanOrEqualOp(Seq(ColumnOp("A", "int"), LiteralOp("1", "int"), LiteralOp("2", "int"))),
        ctx,
        EvalResultUnknown,
        true
      )
      evalTest(
        LessThanOrEqualOp(Seq(ColumnOp("A", "date"), LiteralOp("29", "int"))),
        ctx,
        EvalResultUnknown,
        true
      )
    })

    // All column stats missing.
    evalTest(
      LessThanOrEqualOp(Seq(ColumnOp("A", "date"), LiteralOp("2021-04-29", "date"))),
      EvalContext(Map.empty),
      EvalResultUnknown
    )
  }

  test("timestamp test") {
    // Happy path: GreaterThanOp evaluates to true.
    Seq(
      EvalContext(
        Map.empty,
        Map("A" -> (("2023-06-07T04:27:03.234Z", "2023-07-07T00:00:00.000Z")))
      ),
      EvalContext(
        Map.empty,
        Map("A" -> (("2023-06-10T00:00:00.000Z", "2023-06-10T00:00:00.001Z")))
      )
    ).foreach(ctx => {
      evalTest(
        GreaterThanOp(
          Seq(
            ColumnOp("A", "timestamp"),
            LiteralOp("2023-06-10T00:00:00.000Z", "timestamp")
          )
        ),
        ctx,
        EvalResultTrue
      )
    })

    // Happy path: GreaterThanOrEqualOp evaluates to false.
    Seq(
      EvalContext(
        Map.empty,
        Map("A" -> (("2023-06-07T04:27:03.234Z", "2023-07-07T00:00:00.000Z")))
      ),
      EvalContext(
        Map.empty,
        Map("A" -> (("2023-06-10T00:00:00.000Z", "2023-09-10T00:00:00.000Z")))
      )
    ).foreach(ctx => {
      evalTest(
        GreaterThanOrEqualOp(
          Seq(
            ColumnOp("A", "timestamp"),
            LiteralOp("2023-09-10T00:00:00.010Z", "timestamp")
          )
        ),
        ctx,
        EvalResultFalse
      )

      // Evaluates to unknown in case of missing/incorrect column stats.
      // Some of these will trigger validation failures as well.
      evalTest(
        EqualOp(Seq(ColumnOp("A", "timestamp"), LiteralOp("2023-06-07 17:03.234", "timestamp"))),
        ctx,
        EvalResultUnknown,
        true
      )
      evalTest(
        LessThanOrEqualOp(Seq(ColumnOp("A", "timestamp"), LiteralOp("foo", "timestamp"))),
        ctx,
        EvalResultUnknown,
        true
      )
    })
  }

  test("AndOp test") {
    // The op we will use to test.
    val op = AndOp(
      Seq(
        EqualOp(Seq(ColumnOp("A", "int"), LiteralOp("1", "int"))),
        LessThanOp(Seq(ColumnOp("B", "long"), LiteralOp("25", "long")))
      )
    )

    // Happy path: op evaluates to true for parition and non-partition columns.
    Seq(
      EvalContext(Map("A" -> "1", "B" -> "2")),
      EvalContext(Map("A" -> "1", "B" -> "24")),
      EvalContext(Map.empty, Map("A" -> (("0", "1")), "B" -> (("2", "26")))),
      EvalContext(Map.empty, Map("A" -> (("1", "100")), "B" -> (("24", "100"))))
    ).foreach(ctx => {
      evalTest(op, ctx, EvalResultTrue)
    })

    // Happy path: op evaluates to false for parition and non-partition columns.
    Seq(
      EvalContext(Map("A" -> "2", "B" -> "2")),
      EvalContext(Map("A" -> "1", "B" -> "28")),
      EvalContext(Map("A" -> "2", "B" -> "28")),
      EvalContext(Map.empty, Map("A" -> (("2", "10")), "B" -> (("2", "100")))),
      EvalContext(Map.empty, Map("A" -> (("0", "10")), "B" -> (("26", "100")))),
      EvalContext(Map.empty, Map("A" -> (("2", "10")), "B" -> (("26", "100"))))
    ).foreach(ctx => {
      evalTest(op, ctx, EvalResultFalse)
    })

    // The op evaluates to false in presence of errors, because one of its
    // inputs evaluates to false, and unknown evaluations are suppressed.
    Seq(
      EvalContext(Map("A" -> "2", "B" -> "foo")),
      EvalContext(Map("A" -> "foo", "B" -> "28")),
      EvalContext(Map.empty, Map("A" -> (("2", "10")), "B" -> (("aa", "zz")))),
      EvalContext(Map.empty, Map("A" -> (("a", "b")), "B" -> (("26", "100"))))
    ).foreach(ctx => {
      evalTest(op, ctx, EvalResultFalse)
    })

    // The op evaluates to unknown in presence of errors, because none of
    // of its input evaluates to false.
    Seq(
      EvalContext(Map("A" -> "1", "B" -> "foo")),
      EvalContext(Map("A" -> "foo", "B" -> "24")),
      EvalContext(Map("A" -> "foo", "B" -> "bar")),
      EvalContext(Map.empty, Map("A" -> (("0", "10")), "B" -> (("aa", "zz")))),
      EvalContext(Map.empty, Map("A" -> (("a", "b")), "B" -> (("-1", "100"))))
    ).foreach(ctx => {
      evalTest(op, ctx, EvalResultUnknown)
    })
  }

  test("OrOp test") {
    // The op we will use to test.
    val op = OrOp(
      Seq(
        EqualOp(Seq(ColumnOp("A", "int"), LiteralOp("1", "int"))),
        GreaterThanOp(Seq(ColumnOp("B", "long"), LiteralOp("25", "long")))
      )
    )
    // Happy path: op evaluates to true for parition and non-partition columns.
    Seq(
      EvalContext(Map("A" -> "1", "B" -> "24")),
      EvalContext(Map("A" -> "2", "B" -> "26")),
      EvalContext(Map("A" -> "1", "B" -> "26")),
      EvalContext(Map.empty, Map("A" -> (("0", "1")), "B" -> (("2", "24")))),
      EvalContext(Map.empty, Map("A" -> (("-10", "-1")), "B" -> (("-1", "100")))),
      EvalContext(Map.empty, Map("A" -> (("-10", "10")), "B" -> (("24", "100"))))
    ).foreach(ctx => {
      evalTest(op, ctx, EvalResultTrue)
    })

    // Happy path: op evaluates to false for parition and non-partition columns.
    Seq(
      EvalContext(Map("A" -> "2", "B" -> "2")),
      EvalContext(Map.empty, Map("A" -> (("2", "10")), "B" -> (("-100", "24"))))
    ).foreach(ctx => {
      evalTest(op, ctx, EvalResultFalse)
    })

    // The op evaluates to true in presence of errors, because one of its
    // inputs evaluates to true, and unknown evaluations are suppressed.
    Seq(
      EvalContext(Map("A" -> "1", "B" -> "foo")),
      EvalContext(Map("A" -> "foo", "B" -> "28")),
      EvalContext(Map.empty, Map("A" -> (("-10", "10")), "B" -> (("aa", "zz")))),
      EvalContext(Map.empty, Map("A" -> (("a", "b")), "B" -> (("26", "100"))))
    ).foreach(ctx => {
      evalTest(op, ctx, EvalResultTrue)
    })

    // The op evaluates to unknown in presence of errors, because none of
    // of its input evaluates to true.
    Seq(
      EvalContext(Map("A" -> "2", "B" -> "foo")),
      EvalContext(Map("A" -> "foo", "B" -> "24")),
      EvalContext(Map("A" -> "foo", "B" -> "bar")),
      EvalContext(Map.empty, Map("A" -> (("10", "20")), "B" -> (("aa", "zz")))),
      EvalContext(Map.empty, Map("A" -> (("a", "b")), "B" -> (("1", "10"))))
    ).foreach(ctx => {
      evalTest(op, ctx, EvalResultUnknown)
    })
  }

  test("NotOp test") {
    // The op we will use to test.
    val op = NotOp(Seq(GreaterThanOrEqualOp(Seq(ColumnOp("A", "int"), LiteralOp("7", "int")))))

    // Happy path: op evaluates to true for parition and non-partition columns.
    Seq(
      EvalContext(Map("A" -> "1")),
      EvalContext(Map.empty, Map("A" -> (("2", "5"))))
    ).foreach(ctx => {
      evalTest(op, ctx, EvalResultTrue)
    })

    // Happy path: op evaluates to false for parition and non-partition columns.
    Seq(
      EvalContext(Map("A" -> "7")),
      EvalContext(Map.empty, Map("A" -> (("2", "10"))))
    ).foreach(ctx => {
      evalTest(op, ctx, EvalResultFalse)
    })

    // The op evaluates to unknown in presence of errors.
    Seq(
      EvalContext(Map("A" -> "foo")),
      EvalContext(Map("B" -> "1")),
      EvalContext(Map.empty, Map("A" -> (("aa", "bb")))),
      EvalContext(Map.empty, Map("B" -> (("1", "7"))))
    ).foreach(ctx => {
      evalTest(op, ctx, EvalResultUnknown)
    })
  }

  test("Null test") {
    val op = IsNullOp(Seq(ColumnOp("A", valueType = "string")))

    Seq(
      EvalContext(Map("A" -> "foo")),
      EvalContext(Map.empty, Map("A" -> (("aa", "zz"))))
    ).foreach(ctx => {
      evalTest(op, ctx, EvalResultFalse)
    })

    Seq(
      EvalContext(Map.empty),
      EvalContext(Map("B" -> "foo")),
      EvalContext(Map.empty, Map("B" -> (("aa", "zz"))))
    ).foreach(ctx => {
      evalTest(op, ctx, EvalResultTrue)
    })
  }

  test("Combination tests") {
    // The set of comparison ops we will use in this test.
    val eqOp = EqualOp(Seq(ColumnOp("A", "int"), LiteralOp("1", "int")))
    val ltOp = LessThanOp(Seq(ColumnOp("B", "long"), LiteralOp("25", "long")))
    val lteOp = LessThanOrEqualOp(Seq(ColumnOp("B", "long"), LiteralOp("25", "long")))
    val gtOp = GreaterThanOp(Seq(ColumnOp("C", "string"), LiteralOp("foo", "string")))
    val gteOp = GreaterThanOrEqualOp(Seq(ColumnOp("C", "string"), LiteralOp("foo", "string")))

    // Test Not of And.
    val notOp = NotOp(Seq(AndOp(Seq(eqOp, lteOp))))
    evalTest(notOp, EvalContext(Map("A" -> "1", "B" -> "25")), EvalResultFalse)
    Seq(
      EvalContext(Map("A" -> "1", "B" -> "100")),
      EvalContext(Map("A" -> "0"))
    ).foreach(ctx => {
      evalTest(notOp, ctx, EvalResultTrue)
    })
    Seq(
      EvalContext(Map("A" -> "1")),
      EvalContext(Map.empty)
    ).foreach(ctx => {
      evalTest(notOp, ctx, EvalResultUnknown)
    })

    // Test And of Not of And.
    val andOp = AndOp(Seq(NotOp(Seq(AndOp(Seq(eqOp, lteOp)))), eqOp, gteOp))
    evalTest(andOp, EvalContext(Map("A" -> "1", "B" -> "10", "C" -> "zz")), EvalResultFalse)
    Seq(
      EvalContext(Map("A" -> "1", "C" -> "zz")),
      EvalContext(Map("A" -> "1")),
      EvalContext(Map.empty)
    ).foreach(ctx => {
      evalTest(andOp, ctx, EvalResultUnknown)
    })

    // Test Or of Not of And.
    val orOp = OrOp(Seq(NotOp(Seq(AndOp(Seq(eqOp, lteOp)))), ltOp, gteOp))
    Seq(
      EvalContext(Map("A" -> "1", "B" -> "100", "C" -> "aa")),
      EvalContext(Map("A" -> "1", "C" -> "zz")),
      EvalContext(Map("A" -> "0"))
    ).foreach(ctx => {
      evalTest(orOp, ctx, EvalResultTrue)
    })
    Seq(
      EvalContext(Map("A" -> "1", "C" -> "aa")),
      EvalContext(Map("A" -> "1")),
      EvalContext(Map.empty)
    ).foreach(ctx => {
      evalTest(orOp, ctx, EvalResultUnknown)
    })

    // Test And of Not of Or.
    val andOp2 = AndOp(Seq(NotOp(Seq(OrOp(Seq(eqOp, ltOp)))), gtOp))
    Seq(
      EvalContext(Map("A" -> "0", "C" -> "aa")),
      EvalContext(Map("A" -> "1"))
    ).foreach(ctx => {
      evalTest(andOp2, ctx, EvalResultFalse)
    })
    Seq(
      EvalContext(Map("A" -> "0", "C" -> "zz")),
      EvalContext(Map("A" -> "0")),
      EvalContext(Map.empty)
    ).foreach(ctx => {
      evalTest(andOp2, ctx, EvalResultUnknown)
    })
  }

  test("Error Handling test") {
    // The eqOp1 will trigger an exception all the time due to incorrect format.
    val eqOp1 = EqualOp(Seq(ColumnOp("A", "bool"), LiteralOp("foo", "bool")))
    val ltOp = LessThanOp(Seq(ColumnOp("B", "int"), LiteralOp("25", "int")))
    // The eqOp2 will trigger an error if the specified context is incorrect.
    val eqOp2 = EqualOp(Seq(ColumnOp("C", "long"), LiteralOp("1", "long")))
    val op = AndOp(Seq(eqOp1, ltOp, eqOp2))

    // The callback which will be invoked on exceptions.
    //
    // We expect that after we get more errors than the threshold, the offending
    // sub-tree ops will be turned off.
    var reportErrorCount = 0
    var numTurningOffMsgs = 0
    def reportError(errStr: String): Unit = {
      if (errStr.contains("Turning off")) {
        numTurningOffMsgs = numTurningOffMsgs + 1
      }
      reportErrorCount = reportErrorCount + 1
    }

    val eval = new JsonPredicateEvaluatorV2(op, Some(reportError))

    // Invoke evaluation from the first context until eqOp1 shuts off.
    val ctx1 = EvalContext(Map("A" -> "true", "B" -> "10", "C" -> "0"))
    for (ii <- 1 to eval.kErrorCountThreshold + 1) {
      // The result evaluates to false due to eqOp2.
      assert(eval.evalRaw(ctx1) == EvalResultFalse)
    }
    // Validate that eqOp1 has been turned off.
    assert(reportErrorCount == 2)
    assert(numTurningOffMsgs == 1)
    assert(eval.errorCountMap.get(eqOp1).getOrElse(0) == eval.kErrorCountThreshold)
    assert(eval.errorCountMap.get(eqOp2).getOrElse(0) == 0)
    assert(eval.errorCountMap.get(ltOp).getOrElse(0) == 0)
    assert(eval.errorCountMap.get(op).getOrElse(0) == 0)

    // Invoke evaluation from the second context.
    // This will turn off eqOp2 due to incorrect format on C.
    val ctx2 = EvalContext(Map("A" -> "true", "B" -> "10", "C" -> "foo"))
    for (ii <- 1 to eval.kErrorCountThreshold + 1) {
      // The result evaluates to unknown now.
      assert(eval.evalRaw(ctx2) == EvalResultUnknown)
    }
    // Validate that eqOp2 has also been turned off.
    assert(reportErrorCount == 4)
    assert(numTurningOffMsgs == 2)
    assert(eval.errorCountMap.get(eqOp1).getOrElse(0) == eval.kErrorCountThreshold)
    assert(eval.errorCountMap.get(eqOp2).getOrElse(0) == eval.kErrorCountThreshold)
    assert(eval.errorCountMap.get(ltOp).getOrElse(0) == 0)
    assert(eval.errorCountMap.get(op).getOrElse(0) == 0)

    // With both eqOp1 and eqOp2 turned off, the result is now unknown.
    // We do not see any more errors.
    for (ii <- 1 to eval.kErrorCountThreshold + 1) {
      assert(eval.evalRaw(ctx1) == EvalResultUnknown)
    }
    assert(reportErrorCount == 4)
    assert(eval.errorCountMap.get(eqOp1).getOrElse(0) == eval.kErrorCountThreshold)
    assert(eval.errorCountMap.get(eqOp2).getOrElse(0) == eval.kErrorCountThreshold)
  }

  test("stress test") {
    val op = AndOp(
      Seq(
        GreaterThanOp(Seq(ColumnOp("date", "date"), LiteralOp("2021-04-29", "date"))),
        LessThanOp(Seq(ColumnOp("date", "date"), LiteralOp("2021-05-29", "date")))
      )
    )
    val datePartitionValues = Seq(
      "2021-04-28",
      "2021-04-29",
      "2021-04-30",
      "2021-05-01",
      "2021-05-03",
      "2021-05-04",
      "2021-05-30",
      "2021-05-31",
      "2021-06-01"
    )

    val dateStatsValues = Seq(
      ("2021-04-28", "2021-04-29"),
      ("2021-04-29", "2021-04-30"),
      ("2021-04-30", "2021-05-01"),
      ("2021-05-01", "2021-05-03"),
      ("2021-05-03", "2021-05-04"),
      ("2021-05-04", "2021-05-30"),
      ("2021-05-30", "2021-05-31"),
      ("2021-05-31", "2021-06-01"),
      ("2021-06-01", "2021-06-10")
    )

    val numEval = 1000000

    val start = System.currentTimeMillis()
    val eval = new JsonPredicateEvaluatorV2(op)
    for (i <- 1 to numEval) {
      eval.eval(EvalContext(Map("date" -> datePartitionValues(i % datePartitionValues.size))))
    }
    val t1 = System.currentTimeMillis()

    for (i <- 1 to numEval) {
      eval.eval(EvalContext(Map.empty, Map("date" -> dateStatsValues(i % dateStatsValues.size))))
    }
    val t2 = System.currentTimeMillis()

    val dur1 = t1 - start
    val dur2 = t2 - t1
    // scalastyle:off println
    println("Stress test results(millis): partition_value_dur=" + dur1 + ", stats_dur=" + dur2)
  }
}
