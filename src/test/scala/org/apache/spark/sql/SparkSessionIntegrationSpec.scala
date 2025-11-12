package org.apache.spark.sql

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Integration tests that require a running Spark Connect server.
 *
 * To run these tests:
 * 1. Start Spark Connect server on localhost:15002
 * 2. Run: sbt "testOnly *SparkSessionIntegrationSpec"
 *
 * To skip these tests: sbt "testOnly -- -l Integration"
 */
class SparkSessionIntegrationSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with Matchers
    with BeforeAndAfterAll:

  // Tag for integration tests
  import org.scalatest.tagobjects.Slow

  private var sparkOpt: Option[SparkSession] = None

  override def beforeAll(): Unit =
    super.beforeAll()
    // Check if server is available
    try
      val socket = new java.net.Socket("localhost", 15002)
      socket.close()
    catch
      case _: Exception =>
        info("Spark Connect server not running on localhost:15002 - skipping integration tests")
  end beforeAll

  def withSpark[A](test: SparkSession => IO[A]): IO[A] =
    SparkSession.builder()
      .remote("sc://localhost:15002")
      .appName("IntegrationTest")
      .build()
      .use(test)

  "SparkSession" should "connect to Spark server and get version" taggedAs Slow in {
    withSpark { spark =>
      spark.version.map { version =>
        version should not be empty
        version should include(".")
      }
    }.asserting(_ => succeed)
  }

  "SparkSession.range" should "create a DataFrame with range values" taggedAs Slow in {
    withSpark { spark =>
      for
        df <- spark.range(10)
        rows <- df.collect()
      yield
        rows.length shouldBe 10
        rows.head.getLong(0) shouldBe 0L
        rows.last.getLong(0) shouldBe 9L
    }.asserting(_ => succeed)
  }

  "DataFrame.filter" should "filter rows correctly" taggedAs Slow in {
    withSpark { spark =>
      for
        df <- spark.range(20)
        filtered = df.filter(functions.col("id") < 5)
        rows <- filtered.collect()
      yield
        rows.length shouldBe 5
        assert(rows.forall(_.getLong(0) < 5))
    }.asserting(_ => succeed)
  }

  "DataFrame.select" should "select and transform columns" taggedAs Slow in {
    withSpark { spark =>
      for
        df <- spark.range(5)
        selected = df.select(
          functions.col("id"),
          (functions.col("id") * 2).as("doubled")
        )
        rows <- selected.collect()
      yield
        rows.length shouldBe 5
        rows.foreach { row =>
          val id = row.getLong(0)
          val doubled = row.getLong(1)
          doubled shouldBe id * 2
        }
    }.asserting(_ => succeed)
  }

  "DataFrame.groupBy" should "aggregate data correctly" taggedAs Slow in {
    withSpark { spark =>
      for
        df <- spark.range(20)
        grouped = df
          .groupBy((functions.col("id") % 5).as("remainder"))
          .agg(functions.count(functions.lit(1)).as("count"))
        rows <- grouped.collect()
      yield
        rows.length shouldBe 5
        rows.foreach { row =>
          row.getLong(1) shouldBe 4L
        }
    }.asserting(_ => succeed)
  }

  "DataFrame.orderBy" should "sort rows correctly" taggedAs Slow in {
    withSpark { spark =>
      for
        df <- spark.range(10)
        sorted = df.orderBy(functions.col("id").desc)
        rows <- sorted.collect()
      yield
        rows.length shouldBe 10
        rows.head.getLong(0) shouldBe 9L
        rows.last.getLong(0) shouldBe 0L
        // Verify descending order
        rows.sliding(2).foreach { case Seq(a, b) =>
          a.getLong(0) should be >= b.getLong(0)
        }
    }.asserting(_ => succeed)
  }

  "DataFrame.limit" should "limit number of rows" taggedAs Slow in {
    withSpark { spark =>
      for
        df <- spark.range(100)
        limited = df.limit(5)
        rows <- limited.collect()
      yield
        rows.length shouldBe 5
    }.asserting(_ => succeed)
  }

  "DataFrame.withColumn" should "add computed column" taggedAs Slow in {
    withSpark { spark =>
      for
        df <- spark.range(5)
        withCol = df.withColumn("squared", functions.col("id") * functions.col("id"))
        rows <- withCol.collect()
      yield
        rows.length shouldBe 5
        rows.foreach { row =>
          val id = row.getLong(0)
          val squared = row.getLong(1)
          squared shouldBe id * id
        }
    }.asserting(_ => succeed)
  }

  "DataFrame.join" should "join two DataFrames" taggedAs Slow in {
    withSpark { spark =>
      for
        left <- spark.range(8)
        leftDf = left.select(functions.col("id").as("left_id"))
        right <- spark.range(5, 12)
        rightDf = right.select(functions.col("id").as("right_id"))
        joined = leftDf.join(
          rightDf,
          functions.col("left_id") === functions.col("right_id"),
          "inner"
        )
        rows <- joined.collect()
      yield
        rows.length shouldBe 3  // IDs 5, 6, 7 match
        rows.foreach { row =>
          row.getLong(0) shouldBe row.getLong(1)
        }
    }.asserting(_ => succeed)
  }

  "DataFrame.union" should "combine two DataFrames" taggedAs Slow in {
    withSpark { spark =>
      for
        df1 <- spark.range(5)
        df2 <- spark.range(5, 10)
        unioned = df1.union(df2)
        rows <- unioned.collect()
      yield
        rows.length shouldBe 10
        rows.head.getLong(0) shouldBe 0L
        rows.last.getLong(0) shouldBe 9L
    }.asserting(_ => succeed)
  }

  "SparkSession.sql" should "execute SQL query" taggedAs Slow in {
    withSpark { spark =>
      for
        df <- spark.sql("SELECT id, id * 2 as doubled FROM range(5)")
        rows <- df.collect()
      yield
        rows.length shouldBe 5
        rows.foreach { row =>
          val id = row.getLong(0)
          val doubled = row.getLong(1)
          doubled shouldBe id * 2
        }
    }.asserting(_ => succeed)
  }

  "Complex DataFrame operations" should "work correctly" taggedAs Slow in {
    withSpark { spark =>
      for
        df <- spark.range(100)
        result = df
          .filter(functions.col("id") < 50)
          .filter(functions.col("id") % 2 === 0)
          .select(
            functions.col("id"),
            (functions.col("id") * 2).as("doubled"),
            (functions.col("id") * functions.col("id")).as("squared")
          )
          .orderBy(functions.col("id").desc)
          .limit(5)
        rows <- result.collect()
      yield
        rows.length shouldBe 5
        // Should be 48, 46, 44, 42, 40 in descending order
        rows.head.getLong(0) shouldBe 48L
        rows.last.getLong(0) shouldBe 40L
        rows.foreach { row =>
          val id = row.getLong(0)
          val doubled = row.getLong(1)
          val squared = row.getLong(2)
          doubled shouldBe id * 2
          squared shouldBe id * id
          id % 2 shouldBe 0
        }
    }.asserting(_ => succeed)
  }

  "DataFrame.count" should "count rows correctly" taggedAs Slow in {
    withSpark { spark =>
      for
        df <- spark.range(42)
        count <- df.count()
      yield
        count shouldBe 42L
    }.asserting(_ => succeed)
  }

  "DataFrame.first" should "return first row" taggedAs Slow in {
    withSpark { spark =>
      for
        df <- spark.range(10)
        first <- df.first()
      yield
        first.getLong(0) shouldBe 0L
    }.asserting(_ => succeed)
  }

  "DataFrame.head" should "return first n rows" taggedAs Slow in {
    withSpark { spark =>
      for
        df <- spark.range(100)
        rows <- df.head(3)
      yield
        rows.length shouldBe 3
        rows(0).getLong(0) shouldBe 0L
        rows(1).getLong(0) shouldBe 1L
        rows(2).getLong(0) shouldBe 2L
    }.asserting(_ => succeed)
  }

  "DataFrame.take" should "return first n rows" taggedAs Slow in {
    withSpark { spark =>
      for
        df <- spark.range(50)
        rows <- df.take(7)
      yield
        rows.length shouldBe 7
    }.asserting(_ => succeed)
  }
