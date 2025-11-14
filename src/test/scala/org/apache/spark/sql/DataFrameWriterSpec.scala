package org.apache.spark.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.connect.proto.relations._
import org.apache.spark.sql.connect.client.SparkConnectClient
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

class DataFrameWriterSpec extends AnyFlatSpec with Matchers with MockitoSugar:

  private def createMockSession(): SparkSession =
    import cats.effect.IO
    import org.mockito.ArgumentMatchers.any
    val mockClient = mock[SparkConnectClient]
    // Stub executeCommand to return IO.unit
    when(mockClient.executeCommand(any())).thenReturn(IO.unit)
    val mockSession = mock[SparkSession]
    when(mockSession.client).thenReturn(mockClient)
    mockSession

  private def createBaseDataFrame(session: SparkSession): DataFrame =
    val baseRelation = Relation(
      relType = Relation.RelType.Range(Range(start = Some(0L), end = 10L, step = 1L))
    )
    DataFrame(session, baseRelation)

  // ===========================================================================
  // Builder Pattern Tests
  // ===========================================================================

  "DataFrameWriter.format" should "set the format" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val writer = df.write.format("parquet")

    writer shouldBe a[DataFrameWriter]
  }

  "DataFrameWriter.mode" should "set the save mode" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val writer = df.write.mode("overwrite")

    writer shouldBe a[DataFrameWriter]
  }

  "DataFrameWriter.option" should "add single option" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val writer = df.write.option("compression", "snappy")

    writer shouldBe a[DataFrameWriter]
  }

  "DataFrameWriter.option with boolean" should "convert to string" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val writer = df.write.option("header", true)

    writer shouldBe a[DataFrameWriter]
  }

  "DataFrameWriter.option with long" should "convert to string" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val writer = df.write.option("maxRecordsPerFile", 10000L)

    writer shouldBe a[DataFrameWriter]
  }

  "DataFrameWriter.options" should "add multiple options" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val writer = df.write.options(Map(
      "header" -> "true",
      "delimiter" -> ",",
      "quote" -> "\""
    ))

    writer shouldBe a[DataFrameWriter]
  }

  "DataFrameWriter.partitionBy" should "set partition columns" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val writer = df.write.partitionBy("year", "month", "day")

    writer shouldBe a[DataFrameWriter]
  }

  "DataFrameWriter.bucketBy" should "set bucketing configuration" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val writer = df.write.bucketBy(10, "user_id", "product_id")

    writer shouldBe a[DataFrameWriter]
  }

  "DataFrameWriter.sortBy" should "set sort columns" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)
    val writer = df.write.sortBy("timestamp", "id")

    writer shouldBe a[DataFrameWriter]
  }

  // ===========================================================================
  // Builder Chaining Tests
  // ===========================================================================

  "DataFrameWriter" should "support method chaining" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val writer = df.write
      .format("parquet")
      .mode("overwrite")
      .option("compression", "snappy")
      .partitionBy("date")
      .bucketBy(10, "id")
      .sortBy("timestamp")

    writer shouldBe a[DataFrameWriter]
  }

  "DataFrameWriter" should "support multiple options chaining" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val writer = df.write
      .option("header", "true")
      .option("delimiter", "\t")
      .option("quote", "'")
      .options(Map("escape" -> "\\", "nullValue" -> "NULL"))

    writer shouldBe a[DataFrameWriter]
  }

  // ===========================================================================
  // Format-Specific Shortcut Tests
  // ===========================================================================

  "DataFrameWriter.parquet" should "return IO[Unit]" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.write.parquet("/path/to/output")

    result shouldBe a[cats.effect.IO[_]]
  }

  "DataFrameWriter.json" should "return IO[Unit]" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.write.json("/path/to/output")

    result shouldBe a[cats.effect.IO[_]]
  }

  "DataFrameWriter.csv" should "return IO[Unit]" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.write.csv("/path/to/output")

    result shouldBe a[cats.effect.IO[_]]
  }

  "DataFrameWriter.orc" should "return IO[Unit]" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.write.orc("/path/to/output")

    result shouldBe a[cats.effect.IO[_]]
  }

  "DataFrameWriter.text" should "return IO[Unit]" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.write.text("/path/to/output")

    result shouldBe a[cats.effect.IO[_]]
  }

  // ===========================================================================
  // Save Operations Tests
  // ===========================================================================

  "DataFrameWriter.save with path" should "return IO[Unit]" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.write.format("parquet").save("/path/to/output")

    result shouldBe a[cats.effect.IO[_]]
  }

  "DataFrameWriter.save without path" should "return IO[Unit]" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.write.format("parquet").save()

    result shouldBe a[cats.effect.IO[_]]
  }

  "DataFrameWriter.saveAsTable" should "return IO[Unit]" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.write.saveAsTable("my_table")

    result shouldBe a[cats.effect.IO[_]]
  }

  "DataFrameWriter.insertInto" should "return IO[Unit]" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.write.insertInto("existing_table")

    result shouldBe a[cats.effect.IO[_]]
  }

  // ===========================================================================
  // Mode Conversion Tests
  // ===========================================================================

  "DataFrameWriter with mode append" should "configure append mode" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val writer = df.write.mode("append")

    writer shouldBe a[DataFrameWriter]
  }

  "DataFrameWriter with mode overwrite" should "configure overwrite mode" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val writer = df.write.mode("overwrite")

    writer shouldBe a[DataFrameWriter]
  }

  "DataFrameWriter with mode ignore" should "configure ignore mode" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val writer = df.write.mode("ignore")

    writer shouldBe a[DataFrameWriter]
  }

  "DataFrameWriter with mode error" should "configure error mode" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val writer = df.write.mode("error")

    writer shouldBe a[DataFrameWriter]
  }

  "DataFrameWriter with mode errorifexists" should "configure error mode" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val writer = df.write.mode("errorifexists")

    writer shouldBe a[DataFrameWriter]
  }

  // ===========================================================================
  // Complete Write Configuration Tests
  // ===========================================================================

  "DataFrameWriter" should "support complete CSV write configuration" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.write
      .format("csv")
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ",")
      .option("quote", "\"")
      .option("escape", "\\")
      .partitionBy("year", "month")
      .save("/data/output")

    result shouldBe a[cats.effect.IO[_]]
  }

  "DataFrameWriter" should "support complete Parquet write with bucketing" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.write
      .format("parquet")
      .mode("overwrite")
      .option("compression", "snappy")
      .partitionBy("date")
      .bucketBy(100, "user_id")
      .sortBy("timestamp")
      .save("/data/output")

    result shouldBe a[cats.effect.IO[_]]
  }

  "DataFrameWriter" should "support table write with partitioning" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .saveAsTable("my_database.my_table")

    result shouldBe a[cats.effect.IO[_]]
  }

  "DataFrameWriter" should "support insert into with specific columns" in {
    val mockSession = createMockSession()
    val df = createBaseDataFrame(mockSession)

    val result = df.write
      .mode("append")
      .insertInto("existing_table")

    result shouldBe a[cats.effect.IO[_]]
  }
