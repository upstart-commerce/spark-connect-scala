import cats.effect.{IO, IOApp, ExitCode}
import org.apache.spark.sql.{SparkSession, functions}

/**
 * Advanced DataFrame operations example.
 *
 * Demonstrates:
 * - GroupBy and aggregations
 * - Window functions
 * - Set operations (union, intersect, except)
 * - Column operations
 * - Null handling
 */
object DataFrameExample extends IOApp:

  override def run(args: List[String]): IO[ExitCode] =
    SparkSession.builder()
      .remote("sc://localhost:15002")
      .appName("DataFrameExample")
      .config("spark.sql.shuffle.partitions", "8")
      .build()
      .use { spark =>
        for
          _ <- IO.println("=== DataFrame Operations Example ===\n")

          // Create sample data using range
          _ <- IO.println("Creating sample data...")
          df <- spark.range(1, 101)

          // Add computed columns
          enriched = df
            .withColumn("value", functions.col("id") * 10)
            .withColumn("category", functions.col("id") % 3)
            .withColumn("is_even", functions.col("id") % 2 === 0)

          _ <- IO.println("\nSample data:")
          _ <- enriched.limit(10).show()

          // GroupBy and aggregation
          _ <- IO.println("\nGroupBy and aggregation:")
          grouped = enriched
            .groupBy("category")
            .agg(
              functions.count(functions.lit(1)).as("count"),
              functions.sum(functions.col("value")).as("total_value"),
              functions.avg(functions.col("value")).as("avg_value"),
              functions.min(functions.col("id")).as("min_id"),
              functions.max(functions.col("id")).as("max_id")
            )
          _ <- grouped.show()

          // Multiple grouping columns
          _ <- IO.println("\nMultiple grouping columns:")
          multiGroup = enriched
            .groupBy("category", "is_even")
            .count()
            .sort(functions.col("category"), functions.col("is_even"))
          _ <- multiGroup.show()

          // Filtering with complex conditions
          _ <- IO.println("\nFiltering with complex conditions:")
          filtered = enriched.filter(
            (functions.col("id") > 20) &&
            (functions.col("id") < 80) &&
            (functions.col("category") === 1)
          )
          _ <- filtered.show()

          // Set operations
          _ <- IO.println("\nSet operations:")
          set1 <- spark.range(1, 11)
          set2 <- spark.range(5, 16)

          _ <- IO.println("Union:")
          union = set1.union(set2)
          _ <- union.show()

          _ <- IO.println("Intersect:")
          intersect = set1.intersect(set2)
          _ <- intersect.show()

          _ <- IO.println("Except:")
          except = set1.except(set2)
          _ <- except.show()

          // Distinct and drop duplicates
          _ <- IO.println("\nDistinct values:")
          withDupes <- spark.range(10)
          duplicated = withDupes.union(withDupes).union(withDupes)
          distinct = duplicated.distinct()
          count <- distinct.count()
          _ <- IO.println(s"After removing duplicates: $count rows")

          // Column operations
          _ <- IO.println("\nColumn operations:")
          calculated <- spark.range(10)
          withCalcs = calculated
            .withColumn("squared", functions.col("id") * functions.col("id"))
            .withColumn("cubed", functions.pow(functions.col("id"), 3.0))
            .withColumn("sqrt", functions.sqrt(functions.col("id")))
            .withColumn("abs_diff", functions.abs(functions.col("id") - 5))
          _ <- withCalcs.show()

          // String operations (if we had string data)
          _ <- IO.println("\nString operations example:")
          withStrings <- spark.range(5)
          withStringCol = withStrings
            .withColumn("text", functions.concat(functions.lit("ID_"), functions.col("id")))
            .withColumn("upper", functions.upper(functions.col("text")))
            .withColumn("length", functions.length(functions.col("text")))
          _ <- withStringCol.show()

          // Sorting
          _ <- IO.println("\nSorting:")
          toSort <- spark.range(20)
          sorted = toSort
            .withColumn("random", functions.col("id") % 7)
            .sort(functions.col("random").desc, functions.col("id").asc)
          _ <- sorted.show()

          // Limit and offset simulation
          _ <- IO.println("\nPagination example:")
          data <- spark.range(100)
          page1 = data.limit(10)
          _ <- IO.println("First page (10 rows):")
          _ <- page1.show()

          _ <- IO.println("\n=== DataFrame example completed! ===")
        yield ExitCode.Success
      }
