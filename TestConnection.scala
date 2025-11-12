import cats.effect.{IO, IOApp, ExitCode}
import org.apache.spark.sql.{SparkSession, functions}

/**
 * Simple test to verify connection to Spark Connect server.
 */
object TestConnection extends IOApp:

  override def run(args: List[String]): IO[ExitCode] =
    SparkSession.builder()
      .remote("sc://localhost:15002")
      .appName("TestConnection")
      .build()
      .use { spark =>
        for
          _ <- IO.println("=== Testing Spark Connect Connection ===\n")

          // Get Spark version
          version <- spark.version
          _ <- IO.println(s"✓ Connected to Spark version: $version\n")

          // Example 1: Create a simple range
          _ <- IO.println("Example 1: Creating a range DataFrame")
          df1 <- spark.range(10)
          _ <- df1.show(5)

          // Example 2: Simple filter
          _ <- IO.println("\nExample 2: Filter")
          df2 = df1.filter(functions.col("id") < 5)
          _ <- df2.show()

          // Example 3: Select with expression
          _ <- IO.println("\nExample 3: Select with expression")
          df3 = df1.select(
            functions.col("id"),
            (functions.col("id") * 2).as("doubled")
          )
          _ <- df3.show()

          _ <- IO.println("\n=== Connection test completed successfully! ===")
        yield ExitCode.Success
      }
