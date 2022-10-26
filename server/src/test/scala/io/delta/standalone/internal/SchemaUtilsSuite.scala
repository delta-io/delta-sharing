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

package io.delta.standalone.internal

// scalastyle:off funsuite

import io.delta.standalone.internal.SchemaUtils.isReadCompatible
import io.delta.standalone.types._
import org.scalatest.FunSuite

class SchemaUtilsSuite extends FunSuite {
  // scalastyle:on funsuite

  /////////////////////////////
  // Read Compatibility Checks
  /////////////////////////////

  /**
   * Tests change of datatype within a schema.
   *  - the make() function is a "factory" function to create schemas that vary only by the
   *    given datatype in a specific position in the schema.
   *  - other tests will call this method with different make() functions to test datatype
   *    incompatibility in all the different places within a schema (in a top-level struct,
   *    in a nested struct, as the element type of an array, etc.)
   */
  def testDatatypeChange(scenario: String)(make: DataType => StructType): Unit = {
    val schemas = Map(
      ("int", make(new IntegerType())),
      ("string", make(new StringType())),
      ("struct", make(new StructType(Array(new StructField("a", new StringType()))))),
      ("array", make(new ArrayType(new IntegerType(), true))), // containsNull
      ("map", make(new MapType(new StringType(), new FloatType(), true))) // valueContainsNull
    )
    test(s"change of datatype should fail read compatibility - $scenario") {
      for (a <- schemas.keys; b <- schemas.keys if a != b) {
        assert(
          !isReadCompatible(schemas(a), schemas(b)),
          s"isReadCompatible should have failed for: ${schemas(a)}, ${schemas(b)}"
        )
      }
    }
  }

  /**
   * Tests change of nullability within a schema (making a field nullable is not allowed,
   * but making a nullable field non-nullable is ok).
   *  - the make() function is a "factory" function to create schemas that vary only by the
   *    nullability (of a field, array elemnt, or map values) in a specific position in the schema.
   *  - other tests will call this method with different make() functions to test nullability
   *    incompatibility in all the different places within a schema (in a top-level struct,
   *    in a nested struct, for the element type of an array, etc.)
   */
  def testNullability(scenario: String)(make: Boolean => StructType): Unit = {
    val nullable = make(true)
    val nonNullable = make(false)
    test(s"relaxed nullability should fail read compatibility - $scenario") {
      assert(!isReadCompatible(nonNullable, nullable))
    }
    test(s"restricted nullability should not fail read compatibility - $scenario") {
      assert(isReadCompatible(nullable, nonNullable))
    }
  }

  /**
   * Tests for fields of a struct: adding/dropping fields, changing nullability, case variation
   *  - The make() function is a "factory" method to produce schemas. It takes a function that
   *    mutates a struct (for example, but adding a column, or it could just not make any change).
   *  - Following tests will call this method with different factory methods, to mutate the
   *    various places where a struct can appear (at the top-level, nested in another struct,
   *    within an array, etc.)
   *  - This allows us to have one shared code to test compatibility of a struct field in all the
   *    different places where it may occur.
   */
  def testColumnVariations(scenario: String)(
    make: (StructType => StructType) => StructType): Unit = {

    // generate one schema without extra column, one with, one nullable, and one with mixed case
    val withoutExtra = make(struct => struct) // produce struct WITHOUT extra field
    val withExtraNullable = make(
      struct => addNewField(struct, new StructField("extra", new StringType()))
    )
    val withExtraMixedCase = make(
      struct => addNewField(struct, new StructField("eXtRa", new StringType()))
    )
    val withExtraNonNullable = make(
      struct => addNewField(struct, new StructField("extra", new StringType(), false))
    )

    test(s"dropping a field should fail read compatibility - $scenario") {
      assert(!isReadCompatible(withExtraNullable, withoutExtra))
    }
    test(s"adding a nullable field should not fail read compatibility - $scenario") {
      assert(isReadCompatible(withoutExtra, withExtraNullable))
    }
    test(s"adding a non-nullable field should not fail read compatibility - $scenario") {
      assert(isReadCompatible(withoutExtra, withExtraNonNullable))
    }
    test(s"case variation of field name should fail read compatibility - $scenario") {
      assert(!isReadCompatible(withExtraNullable, withExtraMixedCase))
    }
    testNullability(scenario)(
      b =>
        make(
          struct => addNewField(struct, new StructField("extra", new StringType(), b))
        )
    )
    testDatatypeChange(scenario)(
      datatype => make(struct => addNewField(struct, new StructField("extra", datatype)))
    )
  }

  private def addNewField(struct: StructType, newField: StructField): StructType = {
    new StructType(Array.concat(struct.getFields, Array(newField)))
  }
  // --------------------------------------------------------------------
  // tests for all kinds of places where a field can appear in a struct
  // --------------------------------------------------------------------

  testColumnVariations("top level")(
    f => f(new StructType(Array(new StructField("a", new IntegerType()))))
  )

  testColumnVariations("nested struct")(
    f =>
      new StructType(
        Array(
          new StructField("a", f(new StructType(Array(new StructField("b", new IntegerType())))))
        )
      )
  )

  testColumnVariations("nested in array")(
    f =>
      new StructType(
        Array(
          new StructField(
            "array",
            new ArrayType(f(new StructType(Array(new StructField("b", new IntegerType())))), true)
          )
        )
      )
  )

  testColumnVariations("nested in map key")(
    f =>
      new StructType(
        Array(
          new StructField(
            "map",
            new MapType(
              f(new StructType(Array(new StructField("b", new IntegerType())))),
              new StringType(),
              true
            )
          )
        )
      )
  )

  testColumnVariations("nested in map value")(
    f =>
      new StructType(
        Array(
          new StructField(
            "map",
            new MapType(
              new StringType(),
              f(new StructType(Array(new StructField("b", new IntegerType())))),
              true
            )
          )
        )
      )
  )

  // --------------------------------------------------------------------
  // tests for data type change in places other than struct
  // --------------------------------------------------------------------

  testDatatypeChange("array element")(
    datatype => new StructType(Array(new StructField("array", new ArrayType(datatype, true))))
  )

  testDatatypeChange("map key")(
    datatype =>
      new StructType(Array(new StructField("map", new MapType(datatype, new StringType(), true))))
  )

  testDatatypeChange("map value")(
    datatype =>
      new StructType(Array(new StructField("map", new MapType(new StringType(), datatype, true))))
  )

  // --------------------------------------------------------------------
  // tests for nullability change in places other than struct
  // --------------------------------------------------------------------

  testNullability("array contains null")(
    b =>
      new StructType(
        Array(new StructField("array", new ArrayType(new StringType(), b)))
      )
  )

  testNullability("map contains null values")(
    b =>
      new StructType(
        Array(
          new StructField(
            "map",
            new MapType(new IntegerType(), new StringType(), b)
          )
        )
      )
  )

  testNullability("map nested in array")(
    b =>
      new StructType(
        Array(
          new StructField(
            "map",
            new ArrayType(new MapType(new IntegerType(), new StringType(), b), true)
          )
        )
      )
  )

  testNullability("array nested in map")(
    b =>
      new StructType(
        Array(
          new StructField(
            "map",
            new MapType(new IntegerType(), new ArrayType(new StringType(), b), true)
          )
        )
      )
  )
}
