package org.apache.spark.sql

import cats.effect.IO
import org.apache.spark.connect.proto.relations.{Relation, RelationCommon, Read}
import scala.collection.mutable

/**
 * Interface for loading data from external storage systems into a DataFrame.
 *
 * DataFrameReader provides methods to configure how data should be read and
 * from what source. It uses a builder pattern to set options before loading.
 *
 * Example:
 * {{{
 *   val df = spark.read
 *     .format("parquet")
 *     .option("path", "/data/file.parquet")
 *     .load()
 * }}}
 */
final class DataFrameReader private[sql] (
  private val session: SparkSession
):
  private var formatValue: Option[String] = None
  private var schemaValue: Option[String] = None
  private val options: mutable.Map[String, String] = mutable.Map.empty

  /**
   * Specify the data source format.
   *
   * @param source the data source format (e.g., "parquet", "json", "csv", "orc")
   * @return this DataFrameReader
   */
  def format(source: String): this.type =
    formatValue = Some(source)
    this

  /**
   * Add an option for the data source.
   *
   * @param key the option key
   * @param value the option value
   * @return this DataFrameReader
   */
  def option(key: String, value: String): this.type =
    options(key) = value
    this

  /**
   * Add an option for the data source.
   *
   * @param key the option key
   * @param value the option value
   * @return this DataFrameReader
   */
  def option(key: String, value: Boolean): this.type =
    option(key, value.toString)

  /**
   * Add an option for the data source.
   *
   * @param key the option key
   * @param value the option value
   * @return this DataFrameReader
   */
  def option(key: String, value: Long): this.type =
    option(key, value.toString)

  /**
   * Add multiple options.
   *
   * @param options a map of options
   * @return this DataFrameReader
   */
  def options(options: Map[String, String]): this.type =
    this.options ++= options
    this

  /**
   * Specify the schema as a DDL-formatted string.
   *
   * @param schemaString the schema in DDL format
   * @return this DataFrameReader
   */
  def schema(schemaString: String): this.type =
    schemaValue = Some(schemaString)
    this

  /**
   * Load data from a path.
   *
   * @param path the file path
   * @return a DataFrame
   */
  def load(path: String): IO[DataFrame] =
    load(Some(path))

  /**
   * Load data.
   *
   * @param pathOption optional file path
   * @return a DataFrame
   */
  def load(pathOption: Option[String] = None): IO[DataFrame] =
    IO.pure {
      val format = formatValue.getOrElse("parquet")
      val allOptions = pathOption match
        case Some(p) => options.toMap + ("path" -> p)
        case None => options.toMap

      val dataSource = Read.DataSource(
        format = Some(format),
        schema = schemaValue,
        options = allOptions
      )

      val relation = Relation(
        common = Some(RelationCommon(planId = Some(System.nanoTime()))),
        relType = Relation.RelType.Read(
          Read(
            readType = Read.ReadType.DataSource(dataSource)
          )
        )
      )

      DataFrame(session, relation)
    }

  /**
   * Load data from a path.
   *
   * @param paths multiple file paths
   * @return a DataFrame
   */
  def load(paths: String*): IO[DataFrame] =
    if paths.isEmpty then
      load(None)
    else
      // For multiple paths, combine them
      option("path", paths.mkString(","))
      load(None)

  /**
   * Load data in Parquet format.
   *
   * @param path the file path
   * @return a DataFrame
   */
  def parquet(path: String): IO[DataFrame] =
    format("parquet").load(path)

  /**
   * Load data in JSON format.
   *
   * @param path the file path
   * @return a DataFrame
   */
  def json(path: String): IO[DataFrame] =
    format("json").load(path)

  /**
   * Load data in CSV format.
   *
   * @param path the file path
   * @return a DataFrame
   */
  def csv(path: String): IO[DataFrame] =
    format("csv").load(path)

  /**
   * Load data in ORC format.
   *
   * @param path the file path
   * @return a DataFrame
   */
  def orc(path: String): IO[DataFrame] =
    format("orc").load(path)

  /**
   * Load data in text format.
   *
   * @param path the file path
   * @return a DataFrame
   */
  def text(path: String): IO[DataFrame] =
    format("text").load(path)

  /**
   * Load a table as a DataFrame.
   *
   * @param tableName the table name
   * @return a DataFrame
   */
  def table(tableName: String): IO[DataFrame] =
    session.table(tableName)

object DataFrameReader:
  private[sql] def apply(session: SparkSession): DataFrameReader =
    new DataFrameReader(session)
