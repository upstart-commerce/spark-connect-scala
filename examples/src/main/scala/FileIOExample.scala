import cats.effect.{IO, IOApp, ExitCode}
import org.apache.spark.sql.{SparkSession, functions}

/**
 * File I/O example demonstrating reading and writing data.
 *
 * Shows:
 * - Reading from various formats (CSV, JSON, Parquet)
 * - Writing to various formats
 * - Reader/Writer options
 * - Save modes
 */
object FileIOExample extends IOApp:

  override def run(args: List[String]): IO[ExitCode] =
    SparkSession.builder()
      .remote("sc://localhost:15002")
      .appName("FileIOExample")
      .build()
      .use { spark =>
        for
          _ <- IO.println("=== File I/O Example ===\n")

          // Create sample data
          _ <- IO.println("Creating sample data...")
          data <- spark.range(100)
          enriched = data
            .withColumn("name", functions.concat(functions.lit("user_"), functions.col("id")))
            .withColumn("age", (functions.col("id") % 50) + 18)
            .withColumn("score", functions.col("id") * 3.14)

          _ <- enriched.show(5)

          // Writing to Parquet
          _ <- IO.println("\nWriting to Parquet format...")
          _ <- enriched.write
            .format("parquet")
            .mode("overwrite")
            .save("/tmp/spark-connect-scala/output.parquet")
          _ <- IO.println("Successfully wrote to Parquet")

          // Writing to JSON
          _ <- IO.println("\nWriting to JSON format...")
          _ <- enriched.write
            .format("json")
            .mode("overwrite")
            .save("/tmp/spark-connect-scala/output.json")
          _ <- IO.println("Successfully wrote to JSON")

          // Writing to CSV with options
          _ <- IO.println("\nWriting to CSV format with options...")
          _ <- enriched.write
            .format("csv")
            .option("header", "true")
            .option("delimiter", ",")
            .mode("overwrite")
            .save("/tmp/spark-connect-scala/output.csv")
          _ <- IO.println("Successfully wrote to CSV")

          // Reading from Parquet
          _ <- IO.println("\nReading from Parquet...")
          readParquet <- spark.read
            .format("parquet")
            .load("/tmp/spark-connect-scala/output.parquet")
          _ <- readParquet.show(5)

          // Reading from JSON
          _ <- IO.println("\nReading from JSON...")
          readJson <- spark.read
            .format("json")
            .load("/tmp/spark-connect-scala/output.json")
          _ <- readJson.show(5)

          // Reading from CSV with options
          _ <- IO.println("\nReading from CSV with options...")
          readCsv <- spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("/tmp/spark-connect-scala/output.csv")
          _ <- readCsv.show(5)

          // Partitioned write
          _ <- IO.println("\nWriting with partitioning...")
          _ <- enriched
            .withColumn("partition_col", functions.col("id") % 10)
            .write
            .format("parquet")
            .partitionBy("partition_col")
            .mode("overwrite")
            .save("/tmp/spark-connect-scala/partitioned_output.parquet")
          _ <- IO.println("Successfully wrote partitioned data")

          _ <- IO.println("\n=== File I/O example completed! ===")
          _ <- IO.println("Note: Write operations are placeholders and may need a running Spark server")
        yield ExitCode.Success
      }
