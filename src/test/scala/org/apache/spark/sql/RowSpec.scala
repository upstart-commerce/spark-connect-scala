package org.apache.spark.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RowSpec extends AnyFlatSpec with Matchers:

  "Row.apply" should "create a row from values" in {
    val row = Row(1, "hello", 3.14, true, null)

    row.size shouldBe 5
    row.get(0) shouldBe 1
    row.get(1) shouldBe "hello"
    row.get(2) shouldBe 3.14
    row.get(3) shouldBe true
    row.isNullAt(4) shouldBe true
  }

  "Row.fromSeq" should "create a row from a sequence" in {
    val values = Seq(10, 20, 30)
    val row = Row.fromSeq(values)

    row.size shouldBe 3
    row.get(0) shouldBe 10
    row.get(1) shouldBe 20
    row.get(2) shouldBe 30
  }

  "Row.getAs" should "get value with correct type" in {
    val row = Row(42, "test", 2.5, true)

    row.getAs[Int](0) shouldBe 42
    row.getAs[String](1) shouldBe "test"
    row.getAs[Double](2) shouldBe 2.5
    row.getAs[Boolean](3) shouldBe true
  }

  "Row.getInt" should "get integer value" in {
    val row = Row(100, 200, 300)

    row.getInt(0) shouldBe 100
    row.getInt(1) shouldBe 200
    row.getInt(2) shouldBe 300
  }

  "Row.getLong" should "get long value" in {
    val row = Row(1000L, 2000L)

    row.getLong(0) shouldBe 1000L
    row.getLong(1) shouldBe 2000L
  }

  "Row.getDouble" should "get double value" in {
    val row = Row(1.5, 2.5, 3.5)

    row.getDouble(0) shouldBe 1.5
    row.getDouble(1) shouldBe 2.5
    row.getDouble(2) shouldBe 3.5
  }

  "Row.getString" should "get string value" in {
    val row = Row("foo", "bar", "baz")

    row.getString(0) shouldBe "foo"
    row.getString(1) shouldBe "bar"
    row.getString(2) shouldBe "baz"
  }

  "Row.getBoolean" should "get boolean value" in {
    val row = Row(true, false, true)

    row.getBoolean(0) shouldBe true
    row.getBoolean(1) shouldBe false
    row.getBoolean(2) shouldBe true
  }

  "Row.isNullAt" should "detect null values" in {
    val row = Row(1, null, "test", null)

    row.isNullAt(0) shouldBe false
    row.isNullAt(1) shouldBe true
    row.isNullAt(2) shouldBe false
    row.isNullAt(3) shouldBe true
  }

  "Row.toSeq" should "convert row to sequence" in {
    val row = Row(1, 2, 3, 4, 5)
    val seq = row.toSeq

    seq shouldBe Seq(1, 2, 3, 4, 5)
  }

  "Row.get" should "throw exception for out of bounds index" in {
    val row = Row(1, 2, 3)

    assertThrows[IndexOutOfBoundsException] {
      row.get(5)
    }

    assertThrows[IndexOutOfBoundsException] {
      row.get(-1)
    }
  }

  "Row.toString" should "provide readable representation" in {
    val row = Row(1, "hello", 3.14)
    val str = row.toString

    str should include("1")
    str should include("hello")
    str should include("3.14")
  }

  "Row with mixed types" should "handle various data types" in {
    val date = new java.sql.Date(System.currentTimeMillis())
    val timestamp = new java.sql.Timestamp(System.currentTimeMillis())

    val row = Row(
      42,                    // Int
      100L,                  // Long
      3.14,                  // Double
      2.5f,                  // Float
      true,                  // Boolean
      "test",                // String
      date,                  // Date
      timestamp,             // Timestamp
      null                   // Null
    )

    row.getInt(0) shouldBe 42
    row.getLong(1) shouldBe 100L
    row.getDouble(2) shouldBe 3.14
    row.getFloat(3) shouldBe 2.5f
    row.getBoolean(4) shouldBe true
    row.getString(5) shouldBe "test"
    row.getDate(6) shouldBe date
    row.getTimestamp(7) shouldBe timestamp
    row.isNullAt(8) shouldBe true
  }
