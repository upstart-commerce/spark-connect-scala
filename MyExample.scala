import cats.effect.{IO, IOApp, ExitCode}
import org.apache.spark.sql.{SparkSession, functions}

/**
 * Custom example demonstrating Spark Connect Scala client.
 */
object MyExample extends IOApp:

  override def run(args: List[String]): IO[ExitCode] =
    SparkSession.builder()
      .remote("sc://localhost:15002")
      .appName("MyExample")
      .build()
      .use { spark =>
        for
          _ <- IO.println("=== My Custom Spark Connect Example ===\n")

          // Get Spark version
          version <- spark.version
          _ <- IO.println(s"Connected to Spark version: $version\n")

          // Create a DataFrame with numbers 0-99
          _ <- IO.println("Creating a DataFrame with range 0-99:")
          df <- spark.range(100)
          _ <- df.show(10)

          // Filter for even numbers less than 20
          _ <- IO.println("\nFiltering for even numbers less than 20:")
          evenNumbers = df
            .filter(functions.col("id") < 20)
            .filter(functions.col("id") % 2 === 0)
          _ <- evenNumbers.show()

          // Add computed columns
          _ <- IO.println("\nAdding computed columns:")
          withColumns = df
            .filter(functions.col("id") < 10)
            .withColumn("doubled", functions.col("id") * 2)
            .withColumn("tripled", functions.col("id") * 3)
            .withColumn("squared", functions.col("id") * functions.col("id"))
          _ <- withColumns.show()

          // Group and aggregate
          _ <- IO.println("\nGrouping by id % 5 and computing aggregations:")
          aggregated = df
            .filter(functions.col("id") < 50)
            .groupBy((functions.col("id") % 5).as("remainder"))
            .agg(
              functions.count(functions.lit(1)).as("count"),
              functions.sum(functions.col("id")).as("sum"),
              functions.avg(functions.col("id")).as("avg"),
              functions.max(functions.col("id")).as("max"),
              functions.min(functions.col("id")).as("min")
            )
            .sort(functions.col("remainder"))
          _ <- aggregated.show()

          // SQL query
          _ <- IO.println("\nRunning a SQL query:")
          sqlDf <- spark.sql("""
            SELECT
              id,
              id * id as square,
              id * id * id as cube
            FROM range(10)
            WHERE id >= 5
          """)
          _ <- sqlDf.show()

          // Join example
          _ <- IO.println("\nJoin example - matching IDs:")
          leftDf <- spark.range(8)
          left = leftDf.select(functions.col("id").as("left_id"))
          rightDf <- spark.range(5, 12)
          right = rightDf.select(functions.col("id").as("right_id"))
          joined = left.join(
            right,
            functions.col("left_id") === functions.col("right_id"),
            "inner"
          )
          _ <- joined.show()

          _ <- IO.println("\n=== Example completed successfully! ===")
        yield ExitCode.Success
      }
