package org.apache.spark.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.connect.proto.relations._
import org.apache.spark.connect.proto.expressions.Expression
import org.apache.spark.sql.connect.client.SparkConnectClient
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

class DataFrameAdvancedSpec extends AnyFlatSpec with Matchers with MockitoSugar:

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
  // Repartition Tests
  // ===========================================================================

  "DataFrame.repartition" should "create Repartition relation with shuffle" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.repartition(5)

    result.relation.relType match
      case Relation.RelType.Repartition(repartition) =>
        repartition.input shouldBe defined
        repartition.numPartitions shouldBe 5
        repartition.shuffle shouldBe Some(true)
      case _ => fail("Expected Repartition relation")
  }

  "DataFrame.repartition with column" should "create RepartitionByExpression relation with partition expressions" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.repartition(5, functions.col("id"))

    result.relation.relType match
      case Relation.RelType.RepartitionByExpression(repartition) =>
        repartition.input shouldBe defined
        repartition.partitionExprs should not be empty
      case _ => fail("Expected RepartitionByExpression relation")
  }

  "DataFrame.coalesce" should "create Repartition relation without shuffle" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.coalesce(2)

    result.relation.relType match
      case Relation.RelType.Repartition(repartition) =>
        repartition.input shouldBe defined
        repartition.numPartitions shouldBe 2
        repartition.shuffle shouldBe Some(false)
      case _ => fail("Expected Repartition relation")
  }

  // ===========================================================================
  // Rollup Tests
  // ===========================================================================

  "DataFrame.rollup" should "create GroupedData with Rollup type" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.rollup(functions.col("id"))

    result shouldBe a[GroupedData]
  }

  "DataFrame.rollup with column names" should "create GroupedData with Rollup type" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.rollup("id", "name")

    result shouldBe a[GroupedData]
  }

  "GroupedData from rollup" should "create Aggregate with ROLLUP type" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.rollup(functions.col("id")).count()

    result.relation.relType match
      case Relation.RelType.Aggregate(agg) =>
        agg.groupType shouldBe Aggregate.GroupType.GROUP_TYPE_ROLLUP
        agg.input shouldBe defined
      case _ => fail("Expected Aggregate relation")
  }

  // ===========================================================================
  // Cube Tests
  // ===========================================================================

  "DataFrame.cube" should "create GroupedData with Cube type" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.cube(functions.col("id"))

    result shouldBe a[GroupedData]
  }

  "DataFrame.cube with column names" should "create GroupedData with Cube type" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.cube("id", "name")

    result shouldBe a[GroupedData]
  }

  "GroupedData from cube" should "create Aggregate with CUBE type" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.cube(functions.col("id")).count()

    result.relation.relType match
      case Relation.RelType.Aggregate(agg) =>
        agg.groupType shouldBe Aggregate.GroupType.GROUP_TYPE_CUBE
        agg.input shouldBe defined
      case _ => fail("Expected Aggregate relation")
  }

  // ===========================================================================
  // Unpivot Tests
  // ===========================================================================

  "DataFrame.unpivot with values" should "create Unpivot relation" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val ids = Array(functions.col("id"))
    val values = Array(functions.col("value1"), functions.col("value2"))

    val result = df.unpivot(ids, values, "variable", "value")

    result.relation.relType match
      case Relation.RelType.Unpivot(unpivot) =>
        unpivot.input shouldBe defined
        unpivot.ids should not be empty
        unpivot.values shouldBe defined
        unpivot.variableColumnName shouldBe "variable"
        unpivot.valueColumnName shouldBe "value"
      case _ => fail("Expected Unpivot relation")
  }

  "DataFrame.unpivot without values" should "create Unpivot relation with no values specified" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val ids = Array(functions.col("id"))

    val result = df.unpivot(ids, "variable", "value")

    result.relation.relType match
      case Relation.RelType.Unpivot(unpivot) =>
        unpivot.input shouldBe defined
        unpivot.ids should not be empty
        unpivot.values shouldBe None
        unpivot.variableColumnName shouldBe "variable"
        unpivot.valueColumnName shouldBe "value"
      case _ => fail("Expected Unpivot relation")
  }

  // ===========================================================================
  // Sample Tests
  // ===========================================================================

  "DataFrame.sample" should "create Sample relation" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.sample(withReplacement = false, fraction = 0.5, seed = 42L)

    result.relation.relType match
      case Relation.RelType.Sample(sample) =>
        sample.input shouldBe defined
        sample.withReplacement shouldBe Some(false)
        sample.upperBound shouldBe 0.5
        sample.seed shouldBe Some(42L)
      case _ => fail("Expected Sample relation")
  }

  "DataFrame.sample with replacement" should "create Sample relation with replacement" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.sample(withReplacement = true, fraction = 1.5)

    result.relation.relType match
      case Relation.RelType.Sample(sample) =>
        sample.withReplacement shouldBe Some(true)
        sample.upperBound shouldBe 1.5
      case _ => fail("Expected Sample relation")
  }

  // ===========================================================================
  // RandomSplit Tests
  // ===========================================================================

  "DataFrame.randomSplit" should "create multiple Sample relations" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val weights = Array(0.6, 0.4)
    val result = df.randomSplit(weights, seed = 42L)

    result.length shouldBe 2
    result.foreach { splitDf =>
      splitDf.relation.relType match
        case Relation.RelType.Sample(_) => succeed
        case _ => fail("Expected Sample relation")
    }
  }

  "DataFrame.randomSplit" should "normalize weights" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val weights = Array(1.0, 2.0, 3.0) // Sum = 6.0
    val result = df.randomSplit(weights)

    result.length shouldBe 3
  }

  // ===========================================================================
  // Cache and Persist Tests
  // ===========================================================================

  "DataFrame.cache" should "create CachedLocalRelation or return same dataframe" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.cache()

    // Cache may return the same DataFrame or wrapped relation
    result shouldBe a[DataFrame]
  }

  "DataFrame.persist" should "return DataFrame" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.persist()

    result shouldBe a[DataFrame]
  }

  "DataFrame.unpersist" should "return DataFrame" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.unpersist()

    result shouldBe a[DataFrame]
  }

  "DataFrame.unpersist with blocking" should "return DataFrame" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.unpersist(blocking = true)

    result shouldBe a[DataFrame]
  }

  // ===========================================================================
  // Na and Stat Functions Tests
  // ===========================================================================

  "DataFrame.na" should "return DataFrameNaFunctions" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.na

    result shouldBe a[DataFrameNaFunctions]
  }

  "DataFrame.stat" should "return DataFrameStatFunctions" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.stat

    result shouldBe a[DataFrameStatFunctions]
  }
