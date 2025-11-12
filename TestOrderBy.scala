import cats.effect.{IO, IOApp, ExitCode}
import org.apache.spark.sql.{SparkSession, functions}

object TestOrderBy extends IOApp:
  override def run(args: List[String]): IO[ExitCode] =
    SparkSession.builder()
      .remote("sc://localhost:15002")
      .appName("TestOrderBy")
      .build()
      .use { spark =>
        for
          _ <- IO.println("=== Testing OrderBy ===")

          // Test 1: Simple ascending
          df1 <- spark.range(10)
          asc = df1.orderBy(functions.col("id").asc)
          rows1 <- asc.collect()
          _ <- IO.println(s"\nAscending order: ${rows1.map(_.getLong(0)).mkString(", ")}")

          // Test 2: Simple descending
          df2 <- spark.range(10)
          desc = df2.orderBy(functions.col("id").desc)
          rows2 <- desc.collect()
          _ <- IO.println(s"Descending order: ${rows2.map(_.getLong(0)).mkString(", ")}")

          // Test 3: With limit
          df3 <- spark.range(20)
          limited = df3.orderBy(functions.col("id").desc).limit(5)
          rows3 <- limited.collect()
          _ <- IO.println(s"Desc + Limit: ${rows3.map(_.getLong(0)).mkString(", ")}")

        yield ExitCode.Success
      }
