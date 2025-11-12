package org.apache.spark.sql.types

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.*

class StructTypeSpec extends AnyFlatSpec with Matchers:

  "StructField" should "create a field with name and dataType" in {
    val field = StructField("id", IntegerType, nullable = false)

    field.name shouldBe "id"
    field.dataType shouldBe IntegerType
    field.nullable shouldBe false
  }

  "StructField" should "be nullable by default" in {
    val field = StructField("name", StringType)

    field.nullable shouldBe true
  }

  "StructType" should "create a schema from fields" in {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))

    schema.fields.length shouldBe 3
    schema.fields(0).name shouldBe "id"
    schema.fields(1).name shouldBe "name"
    schema.fields(2).name shouldBe "age"
  }

  "StructType.fieldNames" should "return field names" in {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("email", StringType)
    ))

    schema.fieldNames shouldBe Seq("id", "name", "email")
  }

  "StructType.fieldIndex" should "return correct index for field name" in {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("email", StringType)
    ))

    schema.fieldIndex("id") shouldBe 0
    schema.fieldIndex("name") shouldBe 1
    schema.fieldIndex("email") shouldBe 2
  }

  "StructType.fieldIndex" should "throw exception for non-existent field" in {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType)
    ))

    assertThrows[IllegalArgumentException] {
      schema.fieldIndex("nonexistent")
    }
  }

  "StructType.apply(fieldName)" should "return field by name" in {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType, nullable = false)
    ))

    val field = schema("name")
    field.name shouldBe "name"
    field.dataType shouldBe StringType
    field.nullable shouldBe false
  }

  "StructType.length" should "return number of fields" in {
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", IntegerType),
      StructField("c", IntegerType)
    ))

    schema.size shouldBe 3
  }

  "StructType with complex types" should "support nested structures" in {
    val innerSchema = StructType(Seq(
      StructField("x", DoubleType),
      StructField("y", DoubleType)
    ))

    val schema = StructType(Seq(
      StructField("id", LongType),
      StructField("point", innerSchema),
      StructField("tags", ArrayType(StringType))
    ))

    schema.fields.length shouldBe 3
    schema.fields(1).dataType shouldBe innerSchema
    schema.fields(2).dataType shouldBe ArrayType(StringType)
  }

  "DataType equality" should "work correctly" in {
    IntegerType shouldBe IntegerType
    StringType shouldBe StringType
    DoubleType shouldBe DoubleType

    IntegerType should not be StringType
    ArrayType(IntegerType) shouldBe ArrayType(IntegerType)
    ArrayType(IntegerType) should not be ArrayType(StringType)
  }

  "DataType.toString" should "provide readable representation" in {
    IntegerType.toString shouldBe "IntegerType"
    StringType.toString shouldBe "StringType"
    DoubleType.toString shouldBe "DoubleType"
    BooleanType.toString shouldBe "BooleanType"
    LongType.toString shouldBe "LongType"
    FloatType.toString shouldBe "FloatType"
    DateType.toString shouldBe "DateType"
    TimestampType.toString shouldBe "TimestampType"
  }

  "ArrayType" should "handle element types correctly" in {
    val arrayType = ArrayType(IntegerType)

    arrayType.elementType shouldBe IntegerType
    arrayType.toString should include("ArrayType")
  }

  "MapType" should "handle key and value types correctly" in {
    val mapType = MapType(StringType, IntegerType)

    mapType.keyType shouldBe StringType
    mapType.valueType shouldBe IntegerType
    mapType.toString should include("MapType")
  }
