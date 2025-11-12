import cats.effect.{IO, IOApp, ExitCode}
import org.apache.spark.sql.{SparkSession, functions}

/**
 * Basic example demonstrating Spark Connect Scala client usage.
 *
 * This example shows:
 * - Creating a SparkSession
 * - Running SQL queries
 * - DataFrame transformations
 * - Aggregations
 * - Filtering and sorting
 */
object BasicExample extends IOApp:

  override def run(args: List[String]): IO[ExitCode] =
    SparkSession.builder()
      .remote("sc://localhost:15002")
      .appName("BasicExample")
      .build()
      .use { spark =>
        for
          _ <- IO.println("=== Spark Connect Scala Client - Basic Example ===\n")

          // Get Spark version
          version <- spark.version
          _ <- IO.println(s"Connected to Spark version: $version\n")

          // Example 1: Create a range DataFrame
          _ <- IO.println("Example 1: Creating a range DataFrame")
          df1 <- spark.range(100)
          _ <- df1.show(10)

          // Example 2: Filter and transform
          _ <- IO.println("\nExample 2: Filter and transform")
          df2 = df1
            .filter(functions.col("id") < 10)
            .select(
              functions.col("id"),
              (functions.col("id") * 2).as("doubled"),
              (functions.col("id") * functions.col("id")).as("squared")
            )
          _ <- df2.show()

          // Example 3: SQL query
          _ <- IO.println("\nExample 3: SQL query")
          df3 <- spark.sql("SELECT id, id * 10 as multiplied FROM range(20) WHERE id > 10")
          _ <- df3.show()

          // Example 4: Aggregation
          _ <- IO.println("\nExample 4: Aggregation")
          df4 <- spark.range(1, 101)
          df5 = df4.groupBy((functions.col("id") % 10).as("group"))
            .agg(
              functions.count(functions.lit(1)).as("count"),
              functions.sum(functions.col("id")).as("sum"),
              functions.avg(functions.col("id")).as("avg")
            )
          _ <- df5.show()

          // Example 5: Join
          _ <- IO.println("\nExample 5: Join operations")
          left <- spark.range(5).select(functions.col("id").as("left_id"))
          right <- spark.range(3, 8).select(functions.col("id").as("right_id"))
          joined = left.join(
            right,
            functions.col("left_id") === functions.col("right_id"),
            "inner"
          )
          _ <- joined.show()

          // Example 6: Complex transformations
          _ <- IO.println("\nExample 6: Complex transformations")
          df6 <- spark.range(20)
          df7 = df6
            .withColumn("category",
              functions.when(functions.col("id") < 5, functions.lit("small"))
                .otherwise(
                  functions.when(functions.col("id") < 15, functions.lit("medium"))
                    .otherwise(functions.lit("large"))
                )
            )
            .filter(functions.col("category") === "medium")
            .sort(functions.col("id").desc)
          _ <- df7.show()

          _ <- IO.println("\n=== Example completed successfully! ===")
        yield ExitCode.Success
      }
