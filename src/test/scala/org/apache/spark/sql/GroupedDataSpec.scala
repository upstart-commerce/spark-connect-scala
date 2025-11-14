package org.apache.spark.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.connect.proto.relations._
import org.apache.spark.connect.proto.expressions.Expression
import org.apache.spark.sql.connect.client.SparkConnectClient
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

class GroupedDataSpec extends AnyFlatSpec with Matchers with MockitoSugar:

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
  // Pivot Tests
  // ===========================================================================

  "GroupedData.pivot with column name" should "create pivoted GroupedData" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val grouped = df.groupBy("category")
    val pivoted = grouped.pivot("month")

    pivoted shouldBe a[GroupedData]
  }

  "GroupedData.pivot with Column" should "create pivoted GroupedData" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val grouped = df.groupBy("category")
    val pivoted = grouped.pivot(functions.col("month"))

    pivoted shouldBe a[GroupedData]
  }

  "GroupedData.pivot with column name and values" should "create pivoted GroupedData with specified values" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val grouped = df.groupBy("category")
    val pivoted = grouped.pivot("month", Seq("Jan", "Feb", "Mar"))

    pivoted shouldBe a[GroupedData]
  }

  "GroupedData.pivot with Column and values" should "create pivoted GroupedData with specified values" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val grouped = df.groupBy("category")
    val pivoted = grouped.pivot(functions.col("month"), Seq(1, 2, 3))

    pivoted shouldBe a[GroupedData]
  }

  "Pivoted GroupedData.agg" should "create Aggregate relation with PIVOT type" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.groupBy("category")
      .pivot("month")
      .agg(functions.sum(functions.col("revenue")))

    result.relation.relType match
      case Relation.RelType.Aggregate(agg) =>
        agg.groupType shouldBe Aggregate.GroupType.GROUP_TYPE_PIVOT
        agg.pivot shouldBe defined
        agg.input shouldBe defined
      case _ => fail("Expected Aggregate relation with PIVOT type")
  }

  "Pivoted GroupedData with values.agg" should "create Aggregate with pivot values" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.groupBy("category")
      .pivot("month", Seq("Jan", "Feb"))
      .agg(functions.count(functions.col("*")))

    result.relation.relType match
      case Relation.RelType.Aggregate(agg) =>
        agg.groupType shouldBe Aggregate.GroupType.GROUP_TYPE_PIVOT
        agg.pivot shouldBe defined
        agg.pivot.get.values should not be empty
      case _ => fail("Expected Aggregate relation with pivot values")
  }

  "GroupedData.pivot on non-groupBy" should "throw exception" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val rollupGrouped = df.rollup("category")

    assertThrows[UnsupportedOperationException] {
      rollupGrouped.pivot("month")
    }
  }

  // ===========================================================================
  // Basic Aggregation Tests
  // ===========================================================================

  "GroupedData.agg" should "create Aggregate relation" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.groupBy("id").agg(functions.sum(functions.col("value")))

    result.relation.relType match
      case Relation.RelType.Aggregate(agg) =>
        agg.groupType shouldBe Aggregate.GroupType.GROUP_TYPE_GROUPBY
        agg.groupingExpressions should not be empty
        agg.aggregateExpressions should not be empty
      case _ => fail("Expected Aggregate relation")
  }

  "GroupedData.agg with Map" should "create Aggregate relation with named aggregations" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.groupBy("id").agg(Map("value" -> "sum", "count" -> "count"))

    result.relation.relType match
      case Relation.RelType.Aggregate(agg) =>
        agg.aggregateExpressions.length shouldBe 2
      case _ => fail("Expected Aggregate relation")
  }

  "GroupedData.count" should "create Aggregate with count" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.groupBy("id").count()

    result.relation.relType match
      case Relation.RelType.Aggregate(agg) =>
        agg.aggregateExpressions should not be empty
      case _ => fail("Expected Aggregate relation")
  }

  "GroupedData.sum" should "create Aggregate with sum" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.groupBy("id").sum("value")

    result.relation.relType match
      case Relation.RelType.Aggregate(agg) =>
        agg.aggregateExpressions should not be empty
      case _ => fail("Expected Aggregate relation")
  }

  "GroupedData.avg" should "create Aggregate with avg" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.groupBy("id").avg("value")

    result.relation.relType match
      case Relation.RelType.Aggregate(agg) =>
        agg.aggregateExpressions should not be empty
      case _ => fail("Expected Aggregate relation")
  }

  "GroupedData.mean" should "be alias for avg" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.groupBy("id").mean("value")

    result.relation.relType match
      case Relation.RelType.Aggregate(agg) =>
        agg.aggregateExpressions should not be empty
      case _ => fail("Expected Aggregate relation")
  }

  "GroupedData.max" should "create Aggregate with max" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.groupBy("id").max("value")

    result.relation.relType match
      case Relation.RelType.Aggregate(agg) =>
        agg.aggregateExpressions should not be empty
      case _ => fail("Expected Aggregate relation")
  }

  "GroupedData.min" should "create Aggregate with min" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.groupBy("id").min("value")

    result.relation.relType match
      case Relation.RelType.Aggregate(agg) =>
        agg.aggregateExpressions should not be empty
      case _ => fail("Expected Aggregate relation")
  }

  // ===========================================================================
  // Multiple Aggregation Tests
  // ===========================================================================

  "GroupedData.agg with multiple columns" should "create Aggregate with multiple aggregations" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.groupBy("category").agg(
      functions.sum(functions.col("revenue")),
      functions.avg(functions.col("quantity")),
      functions.max(functions.col("price"))
    )

    result.relation.relType match
      case Relation.RelType.Aggregate(agg) =>
        agg.aggregateExpressions.length shouldBe 3
      case _ => fail("Expected Aggregate relation")
  }

  "GroupedData with multiple grouping columns" should "create Aggregate with multiple grouping expressions" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.groupBy("category", "region").count()

    result.relation.relType match
      case Relation.RelType.Aggregate(agg) =>
        agg.groupingExpressions.length shouldBe 2
      case _ => fail("Expected Aggregate relation")
  }
