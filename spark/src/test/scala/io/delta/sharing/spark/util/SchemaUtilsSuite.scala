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

package io.delta.sharing.spark.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

import io.delta.sharing.spark.util.SchemaUtils.isReadCompatible

class SchemaUtilsSuite extends SparkFunSuite {
  /////////////////////////////
  // Read Compatibility Checks
  /////////////////////////////

  /**
   * Tests change of datatype within a schema.
   *  - the make() function is a "factory" function to create schemas that vary only by the
   * given datatype in a specific position in the schema.
   *  - other tests will call this method with different make() functions to test datatype
   * incompatibility in all the different places within a schema (in a top-level struct,
   * in a nested struct, as the element type of an array, etc.)
   */
  def testDatatypeChange(scenario: String)(make: DataType => StructType): Unit = {
    val schemas = Map(
      ("int", make(IntegerType)),
      ("string", make(StringType)),
      ("struct", make(new StructType().add("a", StringType))),
      ("array", make(ArrayType(IntegerType))),
      ("map", make(MapType(StringType, FloatType)))
    )
    test(s"change of datatype should fail read compatibility - $scenario") {
      for (a <- schemas.keys; b <- schemas.keys if a != b) {
        assert(!isReadCompatible(schemas(a), schemas(b)),
          s"isReadCompatible should have failed for: ${schemas(a)}, ${schemas(b)}")
      }
    }
    test(s"same of datatype should succeed read compatibility - $scenario") {
      schemas.keys.foreach{ k =>
        assert(isReadCompatible(schemas(k), schemas(k)),
          s"isReadCompatible should have succeeded for the same schema: ${schemas(k)}")
      }
    }
  }

  /**
   * Tests change of nullability within a schema (making a field nullable is not allowed,
   * but making a nullable field non-nullable is ok).
   *  - the make() function is a "factory" function to create schemas that vary only by the
   * nullability (of a field, array elemnt, or map values) in a specific position in the schema.
   *  - other tests will call this method with different make() functions to test nullability
   * incompatibility in all the different places within a schema (in a top-level struct,
   * in a nested struct, for the element type of an array, etc.)
   */
  def testNullability(scenario: String)(make: Boolean => StructType): Unit = {
    val nullable = make(true)
    val nonNullable = make(false)
    test(s"relaxed nullability should fail read compatibility - $scenario") {
      assert(!isReadCompatible(nullable, nonNullable))
    }
    test(s"restricted nullability should not fail read compatibility - $scenario") {
      assert(isReadCompatible(nonNullable, nullable))
    }
  }

  /**
   * Tests for fields of a struct: adding/dropping fields, changing nullability, case variation
   *  - The make() function is a "factory" method to produce schemas. It takes a function that
   * mutates a struct (for example, but adding a column, or it could just not make any change).
   *  - Following tests will call this method with different factory methods, to mutate the
   * various places where a struct can appear (at the top-level, nested in another struct,
   * within an array, etc.)
   *  - This allows us to have one shared code to test compatibility of a struct field in all the
   * different places where it may occur.
   */
  def testColumnVariations(scenario: String)
    (make: (StructType => StructType) => StructType): Unit = {

    // generate one schema without extra column, one with, one nullable, and one with mixed case
    val withoutExtra = make(struct => struct) // produce struct WITHOUT extra field
    val withExtraNullable = make(struct => struct.add("extra", StringType))
    val withExtraMixedCase = make(struct => struct.add("eXtRa", StringType))
    val withExtraNonNullable = make(struct => struct.add("extra", StringType, nullable = false))

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
    testNullability(scenario)(b => make(struct => struct.add("extra", StringType, nullable = b)))
    testDatatypeChange(scenario)(datatype => make(struct => struct.add("extra", datatype)))
  }

  // --------------------------------------------------------------------
  // tests for all kinds of places where a field can appear in a struct
  // --------------------------------------------------------------------

  testColumnVariations("top level")(
    f => f(new StructType().add("a", IntegerType)))

  testColumnVariations("nested struct")(
    f => new StructType()
      .add("a", f(new StructType().add("b", IntegerType))))

  testColumnVariations("nested in array")(
    f => new StructType()
      .add("array", ArrayType(
        f(new StructType().add("b", IntegerType)))))

  testColumnVariations("nested in map key")(
    f => new StructType()
      .add("map", MapType(
        f(new StructType().add("b", IntegerType)),
        StringType)))

  testColumnVariations("nested in map value")(
    f => new StructType()
      .add("map", MapType(
        StringType,
        f(new StructType().add("b", IntegerType)))))

  // --------------------------------------------------------------------
  // tests for data type change in places other than struct
  // --------------------------------------------------------------------

  testDatatypeChange("array element")(
    datatype => new StructType()
      .add("array", ArrayType(datatype)))

  testDatatypeChange("map key")(
    datatype => new StructType()
      .add("map", MapType(datatype, StringType)))

  testDatatypeChange("map value")(
    datatype => new StructType()
      .add("map", MapType(StringType, datatype)))

  // --------------------------------------------------------------------
  // tests for nullability change in places other than struct
  // --------------------------------------------------------------------

  testNullability("array contains null")(
    b => new StructType()
      .add("array", ArrayType(StringType, containsNull = b)))

  testNullability("map contains null values")(
    b => new StructType()
      .add("map", MapType(IntegerType, StringType, valueContainsNull = b)))

  testNullability("map nested in array")(
    b => new StructType()
      .add("map", ArrayType(
        MapType(IntegerType, StringType, valueContainsNull = b))))

  testNullability("array nested in map")(
    b => new StructType()
      .add("map", MapType(
        IntegerType,
        ArrayType(StringType, containsNull = b))))
}
