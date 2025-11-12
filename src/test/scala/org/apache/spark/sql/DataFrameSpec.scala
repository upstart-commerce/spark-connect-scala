package org.apache.spark.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.connect.proto.relations._
import org.apache.spark.connect.proto.expressions.Expression
import org.apache.spark.sql.connect.client.SparkConnectClient
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

class DataFrameSpec extends AnyFlatSpec with Matchers with MockitoSugar:

  "DataFrame.select" should "create Project relation" in {
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)

    val baseRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 10L, step = 1L))
    )
    val df = DataFrame(mockSession, baseRelation)

    val result = df.select(functions.col("id"))

    result.relation.relType match
      case Relation.RelType.Project(project) =>
        project.input shouldBe defined
        project.expressions.length shouldBe 1
      case _ => fail("Expected Project relation")
  }

  "DataFrame.filter" should "create Filter relation" in {
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)

    val baseRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 10L, step = 1L))
    )
    val df = DataFrame(mockSession, baseRelation)

    val result = df.filter(functions.col("id") < 5)

    result.relation.relType match
      case Relation.RelType.Filter(filter) =>
        filter.input shouldBe defined
        filter.condition shouldBe defined
      case _ => fail("Expected Filter relation")
  }

  "DataFrame.where" should "be alias for filter" in {
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)

    val baseRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 10L, step = 1L))
    )
    val df = DataFrame(mockSession, baseRelation)

    val result = df.where(functions.col("id") > 3)

    result.relation.relType match
      case Relation.RelType.Filter(filter) =>
        filter.condition shouldBe defined
      case _ => fail("Expected Filter relation")
  }

  "DataFrame.where with string" should "parse SQL expression" in {
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)

    val baseRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 10L, step = 1L))
    )
    val df = DataFrame(mockSession, baseRelation)

    val result = df.where("id > 5")

    result.relation.relType match
      case Relation.RelType.Filter(filter) =>
        filter.condition shouldBe defined
      case _ => fail("Expected Filter relation")
  }

  "DataFrame.groupBy" should "return GroupedData" in {
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)

    val baseRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 10L, step = 1L))
    )
    val df = DataFrame(mockSession, baseRelation)

    val result = df.groupBy(functions.col("id"))

    result shouldBe a[GroupedData]
  }

  "DataFrame.sort" should "create Sort relation" in {
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)

    val baseRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 10L, step = 1L))
    )
    val df = DataFrame(mockSession, baseRelation)

    val result = df.sort(functions.col("id").desc)

    result.relation.relType match
      case Relation.RelType.Sort(sort) =>
        sort.input shouldBe defined
        sort.order.length shouldBe 1
      case _ => fail("Expected Sort relation")
  }

  "DataFrame.orderBy" should "create Sort relation" in {
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)

    val baseRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 10L, step = 1L))
    )
    val df = DataFrame(mockSession, baseRelation)

    val result = df.orderBy(functions.col("id").asc)

    result.relation.relType match
      case Relation.RelType.Sort(sort) =>
        sort.input shouldBe defined
      case _ => fail("Expected Sort relation")
  }

  "DataFrame.limit" should "create Limit relation" in {
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)

    val baseRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 10L, step = 1L))
    )
    val df = DataFrame(mockSession, baseRelation)

    val result = df.limit(5)

    result.relation.relType match
      case Relation.RelType.Limit(limit) =>
        limit.input shouldBe defined
        limit.limit shouldBe 5
      case _ => fail("Expected Limit relation")
  }

  "DataFrame.withColumn" should "create WithColumns relation" in {
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)

    val baseRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 10L, step = 1L))
    )
    val df = DataFrame(mockSession, baseRelation)

    val result = df.withColumn("doubled", functions.col("id") * 2)

    result.relation.relType match
      case Relation.RelType.WithColumns(withCols) =>
        withCols.input shouldBe defined
        withCols.aliases.length shouldBe 1
        withCols.aliases.head.name shouldBe Seq("doubled")
      case _ => fail("Expected WithColumns relation")
  }

  "DataFrame.withColumnRenamed" should "rename column" in {
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)

    val baseRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 10L, step = 1L))
    )
    val df = DataFrame(mockSession, baseRelation)

    val result = df.withColumnRenamed("id", "identifier")

    result.relation.relType match
      case Relation.RelType.WithColumns(withCols) =>
        withCols.aliases.length shouldBe 1
        withCols.aliases.head.name shouldBe Seq("identifier")
      case _ => fail("Expected WithColumns relation")
  }

  "DataFrame.drop" should "create Drop relation" in {
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)

    val baseRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 10L, step = 1L))
    )
    val df = DataFrame(mockSession, baseRelation)

    val result = df.drop("id")

    result.relation.relType match
      case Relation.RelType.Drop(drop) =>
        drop.input shouldBe defined
        drop.columns.length shouldBe 1
      case _ => fail("Expected Drop relation")
  }

  "DataFrame.distinct" should "create Deduplicate relation" in {
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)

    val baseRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 10L, step = 1L))
    )
    val df = DataFrame(mockSession, baseRelation)

    val result = df.distinct()

    result.relation.relType match
      case Relation.RelType.Deduplicate(dedup) =>
        dedup.input shouldBe defined
      case _ => fail("Expected Deduplicate relation")
  }

  "DataFrame.dropDuplicates" should "create Deduplicate relation with columns" in {
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)

    val baseRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 10L, step = 1L))
    )
    val df = DataFrame(mockSession, baseRelation)

    val result = df.dropDuplicates("id", "name")

    result.relation.relType match
      case Relation.RelType.Deduplicate(dedup) =>
        dedup.input shouldBe defined
        dedup.columnNames.length shouldBe 2
        dedup.columnNames shouldBe Seq("id", "name")
      case _ => fail("Expected Deduplicate relation")
  }

  "DataFrame.join" should "create Join relation" in {
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)

    val leftRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 10L, step = 1L))
    )
    val rightRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(5L), end = 15L, step = 1L))
    )

    val leftDf = DataFrame(mockSession, leftRelation)
    val rightDf = DataFrame(mockSession, rightRelation)

    val result = leftDf.join(rightDf, functions.col("id") === functions.col("id"), "inner")

    result.relation.relType match
      case Relation.RelType.Join(join) =>
        join.left shouldBe defined
        join.right shouldBe defined
        join.joinCondition shouldBe defined
        join.joinType shouldBe Join.JoinType.JOIN_TYPE_INNER
      case _ => fail("Expected Join relation")
  }

  "DataFrame.crossJoin" should "create cross Join relation" in {
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)

    val leftRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 5L, step = 1L))
    )
    val rightRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 3L, step = 1L))
    )

    val leftDf = DataFrame(mockSession, leftRelation)
    val rightDf = DataFrame(mockSession, rightRelation)

    val result = leftDf.crossJoin(rightDf)

    result.relation.relType match
      case Relation.RelType.Join(join) =>
        join.left shouldBe defined
        join.right shouldBe defined
        join.joinType shouldBe Join.JoinType.JOIN_TYPE_CROSS
      case _ => fail("Expected Join relation")
  }

  "DataFrame.union" should "create SetOp relation" in {
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)

    val relation1 = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 5L, step = 1L))
    )
    val relation2 = Relation(
      relType = Relation.RelType.Range(Range(start = Some(5L), end = 10L, step = 1L))
    )

    val df1 = DataFrame(mockSession, relation1)
    val df2 = DataFrame(mockSession, relation2)

    val result = df1.union(df2)

    result.relation.relType match
      case Relation.RelType.SetOp(setOp) =>
        setOp.leftInput shouldBe defined
        setOp.rightInput shouldBe defined
        setOp.setOpType shouldBe SetOperation.SetOpType.SET_OP_TYPE_UNION
      case _ => fail("Expected SetOp relation")
  }

  "DataFrame.intersect" should "create SetOp relation with INTERSECT type" in {
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)

    val relation1 = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 10L, step = 1L))
    )
    val relation2 = Relation(
      relType = Relation.RelType.Range(Range(start = Some(5L), end = 15L, step = 1L))
    )

    val df1 = DataFrame(mockSession, relation1)
    val df2 = DataFrame(mockSession, relation2)

    val result = df1.intersect(df2)

    result.relation.relType match
      case Relation.RelType.SetOp(setOp) =>
        setOp.setOpType shouldBe SetOperation.SetOpType.SET_OP_TYPE_INTERSECT
      case _ => fail("Expected SetOp relation")
  }

  "DataFrame.except" should "create SetOp relation with EXCEPT type" in {
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)

    val relation1 = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 10L, step = 1L))
    )
    val relation2 = Relation(
      relType = Relation.RelType.Range(Range(start = Some(5L), end = 15L, step = 1L))
    )

    val df1 = DataFrame(mockSession, relation1)
    val df2 = DataFrame(mockSession, relation2)

    val result = df1.except(df2)

    result.relation.relType match
      case Relation.RelType.SetOp(setOp) =>
        setOp.setOpType shouldBe SetOperation.SetOpType.SET_OP_TYPE_EXCEPT
      case _ => fail("Expected SetOp relation")
  }

  "DataFrame operations" should "chain correctly" in {
    val mockClient = mock[SparkConnectClient]
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)

    val baseRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 100L, step = 1L))
    )
    val df = DataFrame(mockSession, baseRelation)

    // Chain multiple operations
    val result = df
      .filter(functions.col("id") < 50)
      .select(functions.col("id"), (functions.col("id") * 2).as("doubled"))
      .withColumn("status", functions.lit("active"))
      .orderBy(functions.col("id").desc)
      .limit(10)

    // Verify the final relation is Limit
    result.relation.relType match
      case Relation.RelType.Limit(limit) =>
        limit.limit shouldBe 10
        // Verify it has a nested structure
        limit.input shouldBe defined
      case _ => fail("Expected Limit relation as final operation")
  }
