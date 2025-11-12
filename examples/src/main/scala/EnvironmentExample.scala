import cats.effect.{IO, IOApp, ExitCode}
import org.apache.spark.sql.{SparkSession, functions}

/**
 * Example demonstrating the use of SPARK_REMOTE environment variable.
 *
 * This example shows how the client automatically picks up the connection
 * configuration from the SPARK_REMOTE environment variable, similar to
 * the behavior in spark-connect-go and spark-connect-swift.
 *
 * Run with:
 *   export SPARK_REMOTE="sc://localhost:15002"
 *   sbt "runMain EnvironmentExample"
 *
 * Or with parameters:
 *   export SPARK_REMOTE="sc://localhost:15002;user_id=myuser"
 *   sbt "runMain EnvironmentExample"
 */
object EnvironmentExample extends IOApp:

  override def run(args: List[String]): IO[ExitCode] =
    // Check if SPARK_REMOTE is set
    val sparkRemote = Option(System.getenv("SPARK_REMOTE"))

    val message = sparkRemote match
      case Some(url) => s"Using SPARK_REMOTE: $url"
      case None => "SPARK_REMOTE not set, will use default: sc://localhost:15002"

    IO.println(s"=== SPARK_REMOTE Environment Variable Example ===\n") *>
    IO.println(message) *>
    IO.println() *>
    // Notice: No .remote() call! It will automatically use SPARK_REMOTE
    SparkSession.builder()
      .appName("EnvironmentExample")
      .build()
      .use { spark =>
        for
          _ <- IO.println("Successfully connected to Spark!")

          // Get server version to verify connection
          version <- spark.version
          _ <- IO.println(s"Spark version: $version\n")

          // Simple query to test the connection
          _ <- IO.println("Running a simple query...")
          df <- spark.range(10)
          result = df
            .withColumn("squared", functions.col("id") * functions.col("id"))

          _ <- result.show()

          _ <- IO.println("\n=== Connection priority order ===")
          _ <- IO.println("1. .remote() call (explicit) - highest priority")
          _ <- IO.println("2. SPARK_REMOTE environment variable")
          _ <- IO.println("3. Default: sc://localhost:15002 - lowest priority")

          _ <- IO.println("\n=== Example completed successfully! ===")
        yield ExitCode.Success
      }
      .handleErrorWith { error =>
        IO.println(s"Error connecting to Spark: ${error.getMessage}") *>
        IO.println("\nMake sure:") *>
        IO.println("1. Spark Connect server is running") *>
        IO.println("2. SPARK_REMOTE is set correctly (optional)") *>
        IO.println("   export SPARK_REMOTE=\"sc://localhost:15002\"") *>
        IO.pure(ExitCode.Error)
      }
