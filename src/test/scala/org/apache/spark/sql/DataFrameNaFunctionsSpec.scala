package org.apache.spark.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.connect.proto.relations._
import org.apache.spark.sql.connect.client.SparkConnectClient
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

class DataFrameNaFunctionsSpec extends AnyFlatSpec with Matchers with MockitoSugar:

  private def createMockSession(): SparkSession =
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)
    mockSession

  private def createBaseDataFrame(session: SparkSession): DataFrame =
    val baseRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 10L, step = 1L))
    )
    DataFrame(session, baseRelation)

  // ===========================================================================
  // Drop Tests
  // ===========================================================================

  "DataFrameNaFunctions.drop()" should "create DropNa relation with default parameters" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val na = df.na

    val result = na.drop()

    result.relation.relType match
      case Relation.RelType.DropNa(dropNa) =>
        dropNa.input shouldBe defined
        dropNa.minNonNulls shouldBe defined
      case _ => fail("Expected DropNa relation")
  }

  "DataFrameNaFunctions.drop(how)" should "create DropNa relation with specified how" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val na = df.na

    val result = na.drop("any")

    result.relation.relType match
      case Relation.RelType.DropNa(dropNa) =>
        dropNa.input shouldBe defined
        dropNa.minNonNulls shouldBe defined
      case _ => fail("Expected DropNa relation")
  }

  "DataFrameNaFunctions.drop(cols)" should "create DropNa relation with specified columns" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val na = df.na

    val result = na.drop(Array("col1", "col2"))

    result.relation.relType match
      case Relation.RelType.DropNa(dropNa) =>
        dropNa.input shouldBe defined
        dropNa.cols should contain allOf ("col1", "col2")
      case _ => fail("Expected DropNa relation")
  }

  "DataFrameNaFunctions.drop(how, cols)" should "create DropNa relation with how and columns" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val na = df.na

    val result = na.drop("all", Array("col1", "col2"))

    result.relation.relType match
      case Relation.RelType.DropNa(dropNa) =>
        dropNa.input shouldBe defined
        dropNa.cols should contain allOf ("col1", "col2")
        dropNa.minNonNulls shouldBe Some(1) // "all" means at least 1 non-null
      case _ => fail("Expected DropNa relation")
  }

  "DataFrameNaFunctions.drop(minNonNulls)" should "create DropNa relation with minNonNulls" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val na = df.na

    val result = na.drop(3)

    result.relation.relType match
      case Relation.RelType.DropNa(dropNa) =>
        dropNa.input shouldBe defined
        dropNa.minNonNulls shouldBe Some(3)
      case _ => fail("Expected DropNa relation")
  }

  "DataFrameNaFunctions.drop(minNonNulls, cols)" should "create DropNa relation with minNonNulls and columns" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val na = df.na

    val result = na.drop(2, Array("col1", "col2"))

    result.relation.relType match
      case Relation.RelType.DropNa(dropNa) =>
        dropNa.input shouldBe defined
        dropNa.minNonNulls shouldBe Some(2)
        dropNa.cols should contain allOf ("col1", "col2")
      case _ => fail("Expected DropNa relation")
  }

  // ===========================================================================
  // Fill Tests
  // ===========================================================================

  "DataFrameNaFunctions.fill(Long)" should "create FillNa relation with long value" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val na = df.na

    val result = na.fill(42L)

    result.relation.relType match
      case Relation.RelType.FillNa(fillNa) =>
        fillNa.input shouldBe defined
        fillNa.values should not be empty
      case _ => fail("Expected FillNa relation")
  }

  "DataFrameNaFunctions.fill(Double)" should "create FillNa relation with double value" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val na = df.na

    val result = na.fill(3.14)

    result.relation.relType match
      case Relation.RelType.FillNa(fillNa) =>
        fillNa.input shouldBe defined
        fillNa.values should not be empty
      case _ => fail("Expected FillNa relation")
  }

  "DataFrameNaFunctions.fill(String)" should "create FillNa relation with string value" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val na = df.na

    val result = na.fill("default")

    result.relation.relType match
      case Relation.RelType.FillNa(fillNa) =>
        fillNa.input shouldBe defined
        fillNa.values should not be empty
      case _ => fail("Expected FillNa relation")
  }

  "DataFrameNaFunctions.fill(Boolean)" should "create FillNa relation with boolean value" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val na = df.na

    val result = na.fill(true)

    result.relation.relType match
      case Relation.RelType.FillNa(fillNa) =>
        fillNa.input shouldBe defined
        fillNa.values should not be empty
      case _ => fail("Expected FillNa relation")
  }

  "DataFrameNaFunctions.fill(value, cols)" should "create FillNa relation with value and columns" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val na = df.na

    val result = na.fill(0L, Array("col1", "col2"))

    result.relation.relType match
      case Relation.RelType.FillNa(fillNa) =>
        fillNa.input shouldBe defined
        fillNa.cols should contain allOf ("col1", "col2")
        fillNa.values should not be empty
      case _ => fail("Expected FillNa relation")
  }

  "DataFrameNaFunctions.fill(valueMap)" should "create FillNa relation with value map" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val na = df.na

    val valueMap = Map("col1" -> 42, "col2" -> "default", "col3" -> 3.14)
    val result = na.fill(valueMap)

    result.relation.relType match
      case Relation.RelType.FillNa(fillNa) =>
        fillNa.input shouldBe defined
        fillNa.cols should contain allOf ("col1", "col2", "col3")
        fillNa.values.length shouldBe 3
      case _ => fail("Expected FillNa relation")
  }

  "DataFrameNaFunctions.fill(String, Seq)" should "fill specific columns with string" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val na = df.na

    val result = na.fill("unknown", Seq("col1", "col2"))

    result.relation.relType match
      case Relation.RelType.FillNa(fillNa) =>
        fillNa.input shouldBe defined
        fillNa.cols should contain allOf ("col1", "col2")
      case _ => fail("Expected FillNa relation")
  }

  // ===========================================================================
  // Replace Tests
  // ===========================================================================

  "DataFrameNaFunctions.replace(col, replacement)" should "create NAReplace relation" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val na = df.na

    val replacement = Map(1 -> 100, 2 -> 200)
    val result = na.replace("col1", replacement)

    result.relation.relType match
      case Relation.RelType.Replace(naReplace) =>
        naReplace.input shouldBe defined
        naReplace.cols should contain("col1")
        naReplace.replacements should not be empty
      case _ => fail("Expected Replace relation")
  }

  "DataFrameNaFunctions.replace(cols, replacement)" should "create Replace relation with multiple columns" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val na = df.na

    val replacement = Map("old" -> "new", "bad" -> "good")
    val result = na.replace(Array("col1", "col2"), replacement)

    result.relation.relType match
      case Relation.RelType.Replace(naReplace) =>
        naReplace.input shouldBe defined
        naReplace.cols should contain allOf ("col1", "col2")
        naReplace.replacements.length shouldBe 2
      case _ => fail("Expected Replace relation")
  }

  "DataFrameNaFunctions.replace(replacement)" should "create NAReplace relation for all columns" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val na = df.na

    val replacement: Map[Any, Any] = Map((null: Any) -> "N/A")
    val result = na.replace(replacement)

    result.relation.relType match
      case Relation.RelType.Replace(naReplace) =>
        naReplace.input shouldBe defined
        naReplace.replacements should not be empty
      case _ => fail("Expected Replace relation")
  }

  "DataFrameNaFunctions.replace with Seq" should "replace values in specified columns" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val na = df.na

    val replacement = Map(0.0 -> -1.0, 1.0 -> -1.0)
    val result = na.replace(Seq("metric1", "metric2"), replacement)

    result.relation.relType match
      case Relation.RelType.Replace(naReplace) =>
        naReplace.input shouldBe defined
        naReplace.cols should contain allOf ("metric1", "metric2")
      case _ => fail("Expected Replace relation")
  }
