package org.apache.spark.sql

import cats.effect.IO
import org.apache.spark.connect.proto.commands.{Command, WriteOperation, WriteOperationV2}
import scala.collection.mutable

/**
 * Interface for writing a DataFrame to external storage systems.
 *
 * DataFrameWriter provides methods to configure how data should be written
 * and to what destination. It uses a builder pattern to set options.
 *
 * Example:
 * {{{
 *   df.write
 *     .format("parquet")
 *     .mode("overwrite")
 *     .save("/data/output")
 * }}}
 */
final class DataFrameWriter private[sql] (
  private val dataFrame: DataFrame
):
  private var formatValue: Option[String] = None
  private var modeValue: Option[String] = None
  private val options: mutable.Map[String, String] = mutable.Map.empty
  private var partitionColumns: Seq[String] = Seq.empty
  private var bucketColumns: Seq[String] = Seq.empty
  private var numBuckets: Option[Int] = None
  private var sortColumns: Seq[String] = Seq.empty

  /**
   * Specify the data source format.
   *
   * @param source the data source format (e.g., "parquet", "json", "csv", "orc")
   * @return this DataFrameWriter
   */
  def format(source: String): this.type =
    formatValue = Some(source)
    this

  /**
   * Specify the save mode.
   *
   * @param saveMode the save mode:
   *                 - "append": Append data to existing data
   *                 - "overwrite": Overwrite existing data
   *                 - "error" or "errorifexists": Throw an exception if data already exists
   *                 - "ignore": Silently ignore if data already exists
   * @return this DataFrameWriter
   */
  def mode(saveMode: String): this.type =
    modeValue = Some(saveMode)
    this

  /**
   * Add an option for the data source.
   *
   * @param key the option key
   * @param value the option value
   * @return this DataFrameWriter
   */
  def option(key: String, value: String): this.type =
    options(key) = value
    this

  /**
   * Add an option for the data source.
   *
   * @param key the option key
   * @param value the option value
   * @return this DataFrameWriter
   */
  def option(key: String, value: Boolean): this.type =
    option(key, value.toString)

  /**
   * Add an option for the data source.
   *
   * @param key the option key
   * @param value the option value
   * @return this DataFrameWriter
   */
  def option(key: String, value: Long): this.type =
    option(key, value.toString)

  /**
   * Add multiple options.
   *
   * @param options a map of options
   * @return this DataFrameWriter
   */
  def options(options: Map[String, String]): this.type =
    this.options ++= options
    this

  /**
   * Partition the output by the given columns.
   *
   * @param colNames the column names to partition by
   * @return this DataFrameWriter
   */
  def partitionBy(colNames: String*): this.type =
    partitionColumns = colNames
    this

  /**
   * Bucket the output by the given columns.
   *
   * @param numBuckets the number of buckets
   * @param colName the first column to bucket by
   * @param colNames additional columns to bucket by
   * @return this DataFrameWriter
   */
  def bucketBy(numBuckets: Int, colName: String, colNames: String*): this.type =
    this.numBuckets = Some(numBuckets)
    this.bucketColumns = colName +: colNames
    this

  /**
   * Sort the output within each bucket by the given columns.
   *
   * @param colNames the column names to sort by
   * @return this DataFrameWriter
   */
  def sortBy(colNames: String*): this.type =
    sortColumns = colNames
    this

  /**
   * Save the DataFrame to the specified path.
   *
   * @param path the file path
   * @return an IO effect
   */
  def save(path: String): IO[Unit] =
    save(Some(path))

  /**
   * Save the DataFrame.
   *
   * @param pathOption optional file path
   * @return an IO effect
   */
  def save(pathOption: Option[String] = None): IO[Unit] =
    executeWriteOperation { writeOp =>
      pathOption match
        case Some(p) => writeOp.copy(saveType = WriteOperation.SaveType.Path(p))
        case None => writeOp
    }

  /**
   * Save the DataFrame in Parquet format.
   *
   * @param path the file path
   * @return an IO effect
   */
  def parquet(path: String): IO[Unit] =
    format("parquet").save(path)

  /**
   * Save the DataFrame in JSON format.
   *
   * @param path the file path
   * @return an IO effect
   */
  def json(path: String): IO[Unit] =
    format("json").save(path)

  /**
   * Save the DataFrame in CSV format.
   *
   * @param path the file path
   * @return an IO effect
   */
  def csv(path: String): IO[Unit] =
    format("csv").save(path)

  /**
   * Save the DataFrame in ORC format.
   *
   * @param path the file path
   * @return an IO effect
   */
  def orc(path: String): IO[Unit] =
    format("orc").save(path)

  /**
   * Save the DataFrame in text format.
   *
   * @param path the file path
   * @return an IO effect
   */
  def text(path: String): IO[Unit] =
    format("text").save(path)

  /**
   * Save the DataFrame as a table.
   *
   * @param tableName the table name
   * @return an IO effect
   */
  def saveAsTable(tableName: String): IO[Unit] =
    executeWriteOperation { writeOp =>
      writeOp.copy(
        saveType = WriteOperation.SaveType.Table(
          WriteOperation.SaveTable(
            tableName = tableName,
            saveMethod = WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_SAVE_AS_TABLE
          )
        )
      )
    }

  /**
   * Insert the DataFrame into an existing table.
   *
   * @param tableName the table name
   * @return an IO effect
   */
  def insertInto(tableName: String): IO[Unit] =
    executeWriteOperation { writeOp =>
      writeOp.copy(
        saveType = WriteOperation.SaveType.Table(
          WriteOperation.SaveTable(
            tableName = tableName,
            saveMethod = WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_INSERT_INTO
          )
        )
      )
    }

  /**
   * Execute a write operation with the configured settings.
   *
   * @param f function to configure the WriteOperation builder
   * @return an IO effect
   */
  private def executeWriteOperation(configureSaveType: WriteOperation => WriteOperation): IO[Unit] =
    val saveModeProto = modeValue.getOrElse("error").toLowerCase match
      case "append" => WriteOperation.SaveMode.SAVE_MODE_APPEND
      case "overwrite" => WriteOperation.SaveMode.SAVE_MODE_OVERWRITE
      case "ignore" => WriteOperation.SaveMode.SAVE_MODE_IGNORE
      case "error" | "errorifexists" => WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS
      case _ => WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS

    val baseWriteOp = WriteOperation(
      input = Some(dataFrame.relation),
      source = formatValue,
      mode = saveModeProto,
      sortColumnNames = sortColumns,
      partitioningColumns = partitionColumns,
      bucketBy = numBuckets.map { n =>
        WriteOperation.BucketBy(
          numBuckets = n,
          bucketColumnNames = bucketColumns
        )
      },
      options = options.toMap
    )

    val writeOp = configureSaveType(baseWriteOp)

    val command = Command(
      commandType = Command.CommandType.WriteOperation(writeOp)
    )

    dataFrame.session.client.executeCommand(command)

object DataFrameWriter:
  private[sql] def apply(dataFrame: DataFrame): DataFrameWriter =
    new DataFrameWriter(dataFrame)
