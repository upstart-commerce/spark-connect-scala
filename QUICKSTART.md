# Quick Start Guide

## Project Structure

```
spark-connect-scala/
├── build.sbt                          # Build configuration with dependencies
├── project/
│   ├── build.properties              # sbt version
│   └── plugins.sbt                   # ScalaPB plugin for protobuf generation
├── src/main/
│   ├── protobuf/spark/connect/       # Protocol buffer definitions
│   │   ├── base.proto
│   │   ├── relations.proto
│   │   ├── expressions.proto
│   │   ├── commands.proto
│   │   ├── types.proto
│   │   └── ... (11 proto files total)
│   └── scala/org/apache/spark/sql/
│       ├── SparkSession.scala        # Entry point with builder pattern
│       ├── DataFrame.scala           # DataFrame transformations & actions
│       ├── Column.scala              # Column expressions
│       ├── Row.scala                 # Row representation
│       ├── DataFrameReader.scala     # Read from various sources
│       ├── DataFrameWriter.scala     # Write to various formats
│       ├── GroupedData.scala         # Aggregation operations
│       ├── connect/client/
│       │   └── SparkConnectClient.scala  # gRPC client
│       ├── functions/
│       │   └── package.scala         # 50+ built-in functions
│       └── types/
│           └── StructType.scala      # Type system
├── examples/src/main/scala/
│   ├── BasicExample.scala            # Introduction
│   ├── DataFrameExample.scala        # Advanced operations
│   └── FileIOExample.scala           # Reading/writing files
└── README.md                         # Comprehensive documentation
```

## Getting Started in 3 Steps

### Step 1: Start Spark Connect Server

```bash
# If you have Spark installed
cd $SPARK_HOME
./sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.5.0

# Or using Docker
docker run -p 15002:15002 apache/spark:3.5.0 \
  /opt/spark/sbin/start-connect-server.sh
```

### Step 2: Generate Protocol Buffer Code

```bash
cd spark-connect-scala

# This will generate Scala code from .proto files
sbt compile
```

### Step 3: Run an Example

```bash
# Run the basic example
sbt "runMain BasicExample"

# Or run the DataFrame example
sbt "runMain DataFrameExample"
```

## Minimal Example

Create a file `MyFirstApp.scala`:

```scala
import cats.effect.{IO, IOApp, ExitCode}
import org.apache.spark.sql.{SparkSession, functions}

object MyFirstApp extends IOApp:
  override def run(args: List[String]): IO[ExitCode] =
    SparkSession.builder()
      .remote("sc://localhost:15002")
      .appName("MyFirstApp")
      .build()
      .use { spark =>
        for
          // Create data
          df <- spark.range(100)

          // Transform
          result = df
            .filter(functions.col("id") < 10)
            .withColumn("doubled", functions.col("id") * 2)

          // Show results
          _ <- result.show()
        yield ExitCode.Success
      }
```

Run it:
```bash
sbt "runMain MyFirstApp"
```

### Using SPARK_REMOTE Environment Variable

You can also configure the connection using the `SPARK_REMOTE` environment variable (similar to spark-connect-go and spark-connect-swift):

```bash
# Set the environment variable
export SPARK_REMOTE="sc://localhost:15002"

# Now you can omit .remote() in your code
```

```scala
object MyApp extends IOApp:
  override def run(args: List[String]): IO[ExitCode] =
    SparkSession.builder()
      .appName("MyApp")  // No .remote() needed!
      .build()
      .use { spark =>
        // Your code here
        IO.pure(ExitCode.Success)
      }
```

**Priority order:**
1. `.remote()` call (explicit) - highest priority
2. `SPARK_REMOTE` environment variable
3. Default: `sc://localhost:15002` - lowest priority

## Key Features Implemented

### ✅ Core Components
- **SparkSession**: Builder pattern, configuration management
- **DataFrame**: 30+ transformation methods, 10+ actions
- **Column**: Arithmetic, comparison, logical, string operations
- **SparkConnectClient**: gRPC communication with server

### ✅ Operations
- **Transformations**: select, filter, join, groupBy, sort, etc.
- **Aggregations**: count, sum, avg, min, max, etc.
- **Functions**: 50+ built-in SQL functions
- **I/O**: Read/write Parquet, JSON, CSV, ORC, text

### ✅ Advanced Features
- Resource management with cats.effect.Resource
- Functional programming with IO monad
- Type-safe operations throughout
- Immutable data structures
- Lazy evaluation

## Architecture Highlights

### Functional Programming Principles

1. **Pure Functions**: All transformations are pure
2. **Immutability**: DataFrames are immutable
3. **Effect Management**: Side effects in IO monad
4. **Resource Safety**: Automatic cleanup via Resource
5. **Composability**: Easy composition with for-comprehensions

### Scala 3 Features Used

- Extension methods for clean DSL
- Given/using for type classes
- Enum for type-safe options
- Modern syntax (indentation-based)
- Top-level definitions

## Next Steps

1. **Read the full README**: Comprehensive API documentation
2. **Explore examples**: Three complete example applications
3. **Extend the client**:
   - Complete Arrow integration
   - Add streaming support
   - Implement catalog operations
   - Add ML pipeline support

## Common Operations Cheat Sheet

```scala
// Create session
SparkSession.builder()
  .remote("sc://localhost:15002")
  .appName("MyApp")
  .build()
  .use { spark =>

  // Create DataFrame
  df <- spark.range(100)
  df <- spark.sql("SELECT * FROM table")
  df <- spark.read.parquet("/path/to/data")

  // Transform
  val result = df
    .filter(col("age") > 18)
    .select("name", "age")
    .groupBy("country")
    .count()
    .orderBy(col("count").desc)

  // Action
  _ <- result.show()
  count <- result.count()
  rows <- result.collect()

  // Write
  _ <- result.write
    .mode("overwrite")
    .parquet("/output")

  IO.unit
}
```

## Troubleshooting

### Cannot connect to server
- Check server is running: `netstat -an | grep 15002`
- Verify URL: `sc://localhost:15002`

### Compilation errors
- Regenerate protos: `sbt clean compile`
- Check Scala version: 3.3.4

### Import errors
- Generated code location: `target/scala-3.3.4/src_managed/main/scalapb/`
- Make sure protobuf files are in: `src/main/protobuf/spark/connect/`

## Development

```bash
# Format code
sbt scalafmt

# Run tests
sbt test

# Create JAR
sbt package

# Run specific example
sbt "runMain BasicExample"

# Interactive mode
sbt console
```

## Technology Stack

- **Scala 3.3.4**: Modern Scala with latest features
- **Cats Effect 3.5.4**: Functional effect system
- **gRPC 1.68.1**: Network communication
- **ScalaPB 0.11.17**: Protocol buffer code generation
- **Apache Arrow 18.1.0**: Efficient data serialization
- **sbt 1.10.5**: Build tool

---

Happy Spark Connect coding! 🚀
