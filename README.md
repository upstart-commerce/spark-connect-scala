# Spark Connect Scala Client

A modern, functional Scala 3 client for Apache Spark Connect, enabling remote DataFrame operations with type safety and composability.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage Guide](#usage-guide)
  - [Creating a SparkSession](#creating-a-sparksession)
  - [DataFrame Operations](#dataframe-operations)
  - [Column Expressions](#column-expressions)
  - [Aggregations](#aggregations)
  - [Joins](#joins)
  - [Reading and Writing Data](#reading-and-writing-data)
- [Advanced Topics](#advanced-topics)
- [API Reference](#api-reference)
- [Examples](#examples)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Overview

The Spark Connect Scala Client provides a functional, type-safe interface to Apache Spark Connect servers. Built with Scala 3 and leveraging modern functional programming patterns through Cats Effect, it offers a composable and resource-safe way to interact with Spark remotely.

### What is Spark Connect?

Spark Connect is a client-server architecture for Apache Spark that decouples the client from the Spark driver, providing:

- **Remote execution**: Run Spark jobs from any JVM application
- **Thin client**: No need for a full Spark distribution on the client
- **Language flexibility**: Use Spark from any language with a Connect client
- **Improved isolation**: Better separation between client and cluster

## Features

- **Scala 3 Native**: Built from the ground up for Scala 3 with modern language features
- **Functional Programming**: Leverages Cats Effect for pure functional effects
- **Type-Safe**: Strong typing throughout the API
- **Immutable**: All operations return new instances, no mutable state
- **Resource-Safe**: Automatic resource management using `cats.effect.Resource`
- **Composable**: Easy to compose operations using functional patterns
- **Complete DataFrame API**: Select, filter, join, aggregate, and more
- **Rich Functions Library**: 50+ built-in functions for transformations
- **gRPC-based**: Efficient communication using Protocol Buffers
- **Apache Arrow**: Fast data serialization for optimal performance

## Architecture

The client follows a layered architecture:

```
┌─────────────────────────────────────────┐
│   User Application (Scala 3)           │
├─────────────────────────────────────────┤
│   SparkSession                          │
│   ├─ DataFrame (transformations)       │
│   ├─ Column (expressions)              │
│   ├─ DataFrameReader/Writer            │
│   └─ functions (built-in functions)    │
├─────────────────────────────────────────┤
│   SparkConnectClient                    │
│   └─ gRPC Communication Layer           │
├─────────────────────────────────────────┤
│   Protocol Buffers (Spark Connect)     │
├─────────────────────────────────────────┤
│   Apache Arrow (Data Serialization)    │
├─────────────────────────────────────────┤
│   Network (gRPC/HTTP2)                  │
└─────────────────────────────────────────┘
           ↓
    ┌─────────────┐
    │ Spark Server│
    └─────────────┘
```

### Key Components

1. **SparkSession**: Entry point for all Spark functionality
2. **DataFrame**: Lazy, distributed collection with transformations
3. **Column**: Represents column expressions and operations
4. **SparkConnectClient**: Handles gRPC communication with server
5. **functions**: Built-in Spark SQL functions

## Prerequisites

- **Scala**: 3.3.4 or higher
- **JDK**: 11 or higher
- **sbt**: 1.9.0 or higher
- **Spark Connect Server**: Running Spark 3.4+ with Connect enabled

### Starting a Spark Connect Server

```bash
# Download Spark (if not already installed)
wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xzf spark-3.5.0-bin-hadoop3.tgz
cd spark-3.5.0-bin-hadoop3

# Start Spark Connect server
./sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.5.0
```

The server will start on `localhost:15002` by default.

## Installation

### Using sbt

Add the following to your `build.sbt`:

```scala
libraryDependencies += "org.apache.spark" %% "spark-connect-scala" % "0.1.0-SNAPSHOT"
```

### Building from Source

```bash
git clone <repository-url>
cd spark-connect-scala

# Compile and generate protobuf code
sbt compile

# Run tests
sbt test

# Create a JAR
sbt package
```

## Quick Start

Here's a simple example to get started:

```scala
import cats.effect.{IO, IOApp, ExitCode}
import org.apache.spark.sql.{SparkSession, functions}

object QuickStart extends IOApp:
  override def run(args: List[String]): IO[ExitCode] =
    // Create a SparkSession with Resource for automatic cleanup
    SparkSession.builder()
      .remote("sc://localhost:15002")
      .appName("QuickStart")
      .build()
      .use { spark =>
        for
          // Create a DataFrame with range of numbers
          df <- spark.range(100)

          // Transform: filter and add columns
          result = df
            .filter(functions.col("id") < 10)
            .withColumn("doubled", functions.col("id") * 2)

          // Action: show results
          _ <- result.show()

        yield ExitCode.Success
      }
```

Run the example:

```bash
sbt "runMain QuickStart"
```

## Usage Guide

### Creating a SparkSession

The `SparkSession` is the entry point for all Spark functionality. It's created using a builder pattern:

```scala
import org.apache.spark.sql.SparkSession

// Basic session
val sparkResource = SparkSession.builder()
  .remote("sc://localhost:15002")
  .build()

// Session with configuration
val sparkResource = SparkSession.builder()
  .remote("sc://localhost:15002")
  .appName("MySparkApp")
  .config("spark.sql.shuffle.partitions", "16")
  .config("spark.sql.adaptive.enabled", true)
  .build()

// Use the session (automatically closes resources)
sparkResource.use { spark =>
  // Your Spark operations here
  IO.unit
}
```

#### Connection String Format

The remote URL follows this format:

```
sc://host:port[;param=value;param=value]
```

Parameters:
- `user_id`: User identifier
- `token`: Bearer token for authentication
- `use_ssl`: Enable TLS (true/false)

Example:
```scala
.remote("sc://spark-server:15002;user_id=myuser;token=abc123;use_ssl=true")
```

#### Using SPARK_REMOTE Environment Variable

If you don't specify a remote URL via `.remote()`, the builder will automatically check the `SPARK_REMOTE` environment variable:

```bash
# Set the environment variable
export SPARK_REMOTE="sc://my-spark-server:15002"

# Or with parameters
export SPARK_REMOTE="sc://spark-server:15002;user_id=myuser;token=abc123"
```

```scala
// No need to call .remote() - will use SPARK_REMOTE env var
val sparkResource = SparkSession.builder()
  .appName("MyApp")
  .build()
```

Priority order:
1. Explicitly set via `.remote()` (highest priority)
2. `SPARK_REMOTE` environment variable
3. Default: `sc://localhost:15002` (lowest priority)

### DataFrame Operations

DataFrames represent distributed collections of data organized into named columns.

#### Creating DataFrames

```scala
// From range
df <- spark.range(100)
df <- spark.range(start = 0, end = 100, step = 2)

// From SQL query
df <- spark.sql("SELECT * FROM my_table WHERE age > 25")

// From table
df <- spark.table("my_table")

// Reading from file
df <- spark.read
  .format("parquet")
  .load("/path/to/data.parquet")
```

#### Transformations

Transformations are lazy operations that define a new DataFrame:

```scala
// Select columns
val df2 = df.select("name", "age")
val df3 = df.select(col("name"), col("age"))

// Filter rows
val adults = df.filter(col("age") >= 18)
val adults = df.where(col("age") >= 18)
val adults = df.where("age >= 18")

// Add or modify columns
val df2 = df.withColumn("age_plus_one", col("age") + 1)

// Rename columns
val df2 = df.withColumnRenamed("age", "person_age")

// Drop columns
val df2 = df.drop("age", "salary")

// Remove duplicates
val unique = df.distinct()
val unique = df.dropDuplicates("name", "email")

// Sort
val sorted = df.sort(col("age").desc, col("name").asc)
val sorted = df.orderBy(col("salary").desc)

// Limit rows
val top10 = df.limit(10)
```

#### Actions

Actions trigger computation and return results:

```scala
// Count rows
count <- df.count()

// Show first rows
_ <- df.show()              // Show 20 rows
_ <- df.show(10)            // Show 10 rows
_ <- df.show(10, truncate = false)

// Collect all rows to client
rows <- df.collect()

// Get first row(s)
row <- df.first()
rows <- df.head(5)
rows <- df.take(10)

// Print schema
_ <- df.printSchema()

// Explain query plan
_ <- df.explain()
```

### Column Expressions

Columns represent expressions that can be evaluated on DataFrames:

```scala
import org.apache.spark.sql.functions._

// Create column references
val idCol = col("id")
val nameCol = Column("name")

// Arithmetic operations
val doubled = col("value") * 2
val sum = col("a") + col("b")
val diff = col("a") - col("b")
val product = col("a") * col("b")
val quotient = col("a") / col("b")
val remainder = col("a") % col("b")

// Comparison operations
val isAdult = col("age") >= 18
val isEqual = col("status") === "active"
val notEqual = col("status") =!= "deleted"
val lt = col("score") < 100
val gt = col("score") > 50

// Logical operations
val condition = (col("age") >= 18) && (col("country") === "US")
val condition = (col("status") === "new") || (col("status") === "pending")
val notDeleted = !col("deleted")

// Null handling
val hasValue = col("email").isNotNull
val isMissing = col("phone").isNull

// String operations
val upper = col("name").upper
val contains = col("email").contains("@gmail.com")
val startsWith = col("name").startsWith("John")
val matches = col("email").like("%@example.com")

// Aliasing
val renamed = col("id").as("user_id")
val renamed = col("salary").alias("annual_salary")

// Casting
val asString = col("id").cast("string")
val asInt = col("value").cast("int")

// Sorting
val ascending = col("name").asc
val descending = col("score").desc

// Conditional expressions
val category = when(col("age") < 18, lit("minor"))
  .when(col("age") < 65, lit("adult"))
  .otherwise(lit("senior"))
```

### Aggregations

Group data and compute aggregate statistics:

```scala
// Simple groupBy
val grouped = df.groupBy("category")
val result = grouped.count()

// Multiple grouping columns
val grouped = df.groupBy("category", "region")

// Aggregation functions
val stats = df.groupBy("department").agg(
  count(lit(1)).as("employee_count"),
  sum(col("salary")).as("total_salary"),
  avg(col("salary")).as("avg_salary"),
  min(col("age")).as("min_age"),
  max(col("age")).as("max_age")
)

// Named aggregations
val stats = df.groupBy("department").agg(Map(
  "salary" -> "sum",
  "age" -> "avg",
  "id" -> "count"
))

// Aggregation without grouping
val globalStats = df.agg(
  count(lit(1)).as("total_count"),
  sum(col("revenue")).as("total_revenue")
)
```

### Joins

Combine DataFrames:

```scala
// Inner join
val joined = left.join(right, col("left.id") === col("right.id"))
val joined = left.join(right, col("left.id") === col("right.id"), "inner")

// Left outer join
val joined = left.join(right, col("left.id") === col("right.id"), "left")

// Right outer join
val joined = left.join(right, col("left.id") === col("right.id"), "right")

// Full outer join
val joined = left.join(right, col("left.id") === col("right.id"), "full")

// Cross join (Cartesian product)
val crossed = left.crossJoin(right)

// Semi join (returns left rows with matches)
val semi = left.join(right, col("left.id") === col("right.id"), "semi")

// Anti join (returns left rows without matches)
val anti = left.join(right, col("left.id") === col("right.id"), "anti")
```

### Reading and Writing Data

#### Reading Data

```scala
// Read Parquet
df <- spark.read.parquet("/path/to/data.parquet")

// Read JSON
df <- spark.read.json("/path/to/data.json")

// Read CSV with options
df <- spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", ",")
  .load("/path/to/data.csv")

// Read from multiple files
df <- spark.read.parquet("/path/file1.parquet", "/path/file2.parquet")

// Read with schema
df <- spark.read
  .schema("id INT, name STRING, age INT")
  .json("/path/to/data.json")

// Read ORC
df <- spark.read.orc("/path/to/data.orc")

// Read text file
df <- spark.read.text("/path/to/data.txt")

// Read from table
df <- spark.read.table("my_table")
```

#### Writing Data

```scala
// Write Parquet
_ <- df.write.parquet("/path/to/output.parquet")

// Write JSON
_ <- df.write.json("/path/to/output.json")

// Write CSV with options
_ <- df.write
  .format("csv")
  .option("header", "true")
  .option("delimiter", "|")
  .save("/path/to/output.csv")

// Save modes
_ <- df.write.mode("overwrite").parquet("/path/to/output")
_ <- df.write.mode("append").parquet("/path/to/output")
_ <- df.write.mode("ignore").parquet("/path/to/output")
_ <- df.write.mode("error").parquet("/path/to/output")

// Partitioned write
_ <- df.write
  .partitionBy("year", "month")
  .parquet("/path/to/partitioned")

// Bucketed write
_ <- df.write
  .bucketBy(10, "id")
  .sortBy("timestamp")
  .saveAsTable("bucketed_table")

// Write to table
_ <- df.write.saveAsTable("my_table")
```

## Advanced Topics

### Set Operations

```scala
// Union (combine all rows)
val combined = df1.union(df2)

// Union by name (match columns by name)
val combined = df1.unionByName(df2)

// Intersect (common rows)
val common = df1.intersect(df2)

// Except (rows in df1 but not in df2)
val diff = df1.except(df2)
```

### Working with Complex Types

```scala
// Create arrays
val withArray = df.withColumn("numbers", array(col("a"), col("b"), col("c")))

// Explode arrays
val exploded = df.withColumn("number", explode(col("numbers")))

// Array size
val withSize = df.withColumn("count", size(col("numbers")))

// Access array elements
val first = col("numbers")(0)
val second = col("numbers").getItem(1)
```

### Null Handling

```scala
// Coalesce (first non-null value)
val filled = df.withColumn("value", coalesce(col("a"), col("b"), lit(0)))

// Replace nulls
val filled = df.withColumn("name", nvl(col("name"), lit("Unknown")))

// Filter nulls
val nonNull = df.filter(col("email").isNotNull)
```

## API Reference

### SparkSession

```scala
class SparkSession:
  def sql(sqlText: String): IO[DataFrame]
  def read: DataFrameReader
  def table(tableName: String): IO[DataFrame]
  def range(end: Long): IO[DataFrame]
  def range(start: Long, end: Long): IO[DataFrame]
  def range(start: Long, end: Long, step: Long, numPartitions: Option[Int]): IO[DataFrame]
  def conf(key: String): IO[String]
  def setConf(key: String, value: String): IO[Unit]
  def version: IO[String]
  def stop(): IO[Unit]
```

### DataFrame

```scala
class DataFrame:
  // Transformations
  def select(cols: Column*): DataFrame
  def filter(condition: Column): DataFrame
  def where(condition: Column): DataFrame
  def groupBy(cols: Column*): GroupedData
  def sort(cols: Column*): DataFrame
  def orderBy(cols: Column*): DataFrame
  def limit(n: Int): DataFrame
  def withColumn(colName: String, col: Column): DataFrame
  def withColumnRenamed(existingName: String, newName: String): DataFrame
  def drop(colNames: String*): DataFrame
  def distinct(): DataFrame
  def dropDuplicates(colNames: String*): DataFrame
  def join(right: DataFrame, joinExprs: Column, joinType: String): DataFrame
  def crossJoin(right: DataFrame): DataFrame
  def union(other: DataFrame): DataFrame
  def intersect(other: DataFrame): DataFrame
  def except(other: DataFrame): DataFrame

  // Actions
  def count(): IO[Long]
  def collect(): IO[Seq[Row]]
  def show(numRows: Int, truncate: Boolean): IO[Unit]
  def first(): IO[Row]
  def head(n: Int): IO[Seq[Row]]
  def take(n: Int): IO[Seq[Row]]
  def printSchema(): IO[Unit]
  def schema: IO[StructType]
  def explain(extended: Boolean): IO[Unit]

  // Writers
  def write: DataFrameWriter
```

### Column

```scala
class Column:
  // Arithmetic
  def +(other: Column): Column
  def -(other: Column): Column
  def *(other: Column): Column
  def /(other: Column): Column
  def %(other: Column): Column
  def unary_- : Column

  // Comparison
  def ===(other: Column): Column
  def =!=(other: Column): Column
  def <(other: Column): Column
  def <=(other: Column): Column
  def >(other: Column): Column
  def >=(other: Column): Column

  // Logical
  def &&(other: Column): Column
  def ||(other: Column): Column
  def unary_! : Column

  // Null handling
  def isNull: Column
  def isNotNull: Column
  def isNaN: Column

  // String operations
  def contains(other: Column): Column
  def startsWith(other: Column): Column
  def endsWith(other: Column): Column
  def like(pattern: String): Column
  def rlike(pattern: String): Column

  // Sorting
  def asc: Column
  def desc: Column

  // Aliasing
  def alias(name: String): Column
  def as(name: String): Column

  // Casting
  def cast(dataType: String): Column

  // Collection operations
  def apply(key: Any): Column
  def getItem(key: Any): Column
  def getField(fieldName: String): Column
```

## Examples

The `examples/` directory contains complete, runnable examples:

1. **BasicExample.scala**: Introduction to basic operations
2. **DataFrameExample.scala**: Advanced DataFrame transformations
3. **FileIOExample.scala**: Reading and writing data

Run examples with:

```bash
sbt "runMain BasicExample"
sbt "runMain DataFrameExample"
sbt "runMain FileIOExample"
```

## Best Practices

### 1. Resource Management

Always use `Resource` for automatic cleanup:

```scala
// Good
SparkSession.builder().build().use { spark =>
  // operations
}

// Avoid manually managing resources
val spark = SparkSession.builder().build()
// ... operations ...
spark.stop() // Easy to forget!
```

### 2. Lazy Evaluation

Remember that DataFrame operations are lazy:

```scala
val df2 = df1.filter(col("age") > 18)  // No execution yet
val df3 = df2.select("name", "age")     // Still no execution
val count = df3.count()                  // NOW execution happens
```

### 3. Immutability

DataFrames are immutable - operations return new instances:

```scala
val df = spark.range(10)
df.filter(col("id") < 5)  // Original df unchanged
val filtered = df.filter(col("id") < 5)  // Need to capture result
```

### 4. Type Safety

Leverage Scala's type system:

```scala
// Use strongly typed operations
val result: IO[Long] = df.count()

// Handle effects properly
result.flatMap(count => IO.println(s"Count: $count"))
```

### 5. Column References

Use `col()` or `Column()` for clarity:

```scala
// Clear
df.filter(col("age") > 18)

// Less clear (mixing strings and columns)
df.filter("age > 18")
```

## Troubleshooting

### Connection Issues

**Problem**: Cannot connect to Spark Connect server

```
Solution:
1. Verify server is running: netstat -an | grep 15002
2. Check connection string format
3. Verify firewall rules
4. Check authentication tokens if using security
```

### Compilation Errors

**Problem**: Protobuf generation fails

```bash
# Clean and regenerate
sbt clean
rm -rf target/
sbt compile
```

**Problem**: Import errors for generated classes

```scala
// Ensure protobuf files are in correct location:
// src/main/protobuf/spark/connect/*.proto
```

### Runtime Errors

**Problem**: Operation not yet implemented

```
Some operations may show placeholder messages.
This is a early version - contributions welcome!
```

## Contributing

Contributions are welcome! Areas for improvement:

1. **Arrow Integration**: Complete Apache Arrow serialization/deserialization
2. **Error Handling**: Enhanced error types and retry logic
3. **Streaming**: DataStreamReader/Writer support
4. **Catalog**: Database and table management operations
5. **ML Pipelines**: Machine learning support
6. **Testing**: Integration tests with Spark server
7. **Performance**: Optimizations and benchmarks
8. **Documentation**: Additional examples and guides

### Development Setup

```bash
# Clone repository
git clone <repository-url>
cd spark-connect-scala

# Install dependencies
sbt update

# Compile
sbt compile

# Run tests
sbt test

# Format code
sbt scalafmt
```

## Technical Design

### Functional Programming Principles

This client follows functional programming best practices:

- **Pure Functions**: No side effects in transformation logic
- **Immutable Data Structures**: All operations return new instances
- **Effect Management**: Side effects captured in `IO` monad
- **Resource Safety**: Automatic cleanup via `Resource`
- **Composability**: Operations easily compose using for-comprehensions

### Type Safety

Scala 3 features leveraged:

- **Union Types**: Type-safe alternatives
- **Opaque Types**: Zero-cost abstractions
- **Extension Methods**: Clean DSL syntax
- **Given/Using**: Implicit conversions and type classes
- **Enums**: Type-safe enumerations

### Dependencies

- **Cats Effect 3**: Effect system and resource management
- **gRPC Java**: Network communication
- **ScalaPB**: Protocol buffer code generation
- **Apache Arrow**: Efficient data serialization

## License

Apache License 2.0

## Acknowledgments

This client was inspired by:
- [spark-connect-go](https://github.com/apache/spark-connect-go) - Go implementation
- [spark-connect-swift](https://github.com/apache/spark) - Swift implementation
- [Apache Spark](https://spark.apache.org/) - The original Spark project

## Support

For issues, questions, or contributions:

- GitHub Issues: [Create an issue]
- Documentation: This README and inline code comments
- Examples: See `examples/` directory

---

Built with Scala 3 and functional programming principles.
