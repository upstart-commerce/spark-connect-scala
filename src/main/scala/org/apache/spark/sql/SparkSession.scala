package org.apache.spark.sql

import cats.effect.{IO, Resource}
import org.apache.spark.sql.connect.client.SparkConnectClient

import java.util.UUID

/**
 * The entry point for Spark Connect functionality in Scala.
 *
 * A SparkSession can be created using the builder pattern:
 * {{{
 *   val spark = SparkSession.builder()
 *     .remote("sc://localhost:15002")
 *     .appName("MyApp")
 *     .config("spark.sql.shuffle.partitions", "200")
 *     .build()
 * }}}
 *
 * This class provides methods for:
 * - Creating DataFrames from various sources
 * - Executing SQL queries
 * - Managing configuration
 * - Accessing catalog operations
 */
final class SparkSession private (
  private[sql] val client: SparkConnectClient,
  val sessionId: String
):

  /**
   * Execute a SQL query and return the result as a DataFrame.
   *
   * @param sqlText the SQL query string
   * @return a DataFrame containing the query results
   */
  def sql(sqlText: String): IO[DataFrame] =
    IO.pure(DataFrame.sql(this, sqlText))

  /**
   * Returns a DataFrameReader that can be used to read data from various sources.
   *
   * @return a DataFrameReader instance
   */
  def read: DataFrameReader =
    DataFrameReader(this)

  /**
   * Returns the specified table as a DataFrame.
   *
   * @param tableName the name of the table
   * @return a DataFrame representing the table
   */
  def table(tableName: String): IO[DataFrame] =
    IO.pure(DataFrame.table(this, tableName))

  /**
   * Create a DataFrame with a single column named "id" containing elements in a range.
   *
   * @param end the end value (exclusive)
   * @return a DataFrame containing a range of values
   */
  def range(end: Long): IO[DataFrame] =
    range(0L, end, 1L, None)

  /**
   * Create a DataFrame with a single column named "id" containing elements in a range.
   *
   * @param start the start value (inclusive)
   * @param end the end value (exclusive)
   * @return a DataFrame containing a range of values
   */
  def range(start: Long, end: Long): IO[DataFrame] =
    range(start, end, 1L, None)

  /**
   * Create a DataFrame with a single column named "id" containing elements in a range.
   *
   * @param start the start value (inclusive)
   * @param end the end value (exclusive)
   * @param step the increment step
   * @param numPartitions the number of partitions for the DataFrame
   * @return a DataFrame containing a range of values
   */
  def range(start: Long, end: Long, step: Long, numPartitions: Option[Int]): IO[DataFrame] =
    IO.pure(DataFrame.range(this, start, end, step, numPartitions))

  /**
   * Get the current configuration.
   *
   * @param key the configuration key
   * @return the configuration value
   */
  def conf(key: String): IO[String] =
    client.getConfig(key)

  /**
   * Set a configuration value.
   *
   * @param key the configuration key
   * @param value the configuration value
   */
  def setConf(key: String, value: String): IO[Unit] =
    client.setConfig(key, value)

  /**
   * Returns the Spark server version.
   */
  def version: IO[String] =
    client.version()

  /**
   * Stop this Spark session and release all resources.
   */
  def stop(): IO[Unit] =
    client.close()

object SparkSession:

  /**
   * Creates a SparkSession.Builder for configuring and creating sessions.
   *
   * @return a new Builder instance
   */
  def builder(): Builder = new Builder()

  /**
   * Builder for SparkSession with a fluent API.
   * Follows the builder pattern seen in Go and Swift implementations.
   */
  final class Builder private[SparkSession] ():
    private var remoteUrl: Option[String] = None
    private var appNameValue: Option[String] = None
    private var configs: Map[String, String] = Map.empty
    private var sessionIdValue: Option[String] = None

    /**
     * Sets the remote Spark Connect server URL.
     *
     * Format: sc://host:port[;param=value]
     *
     * @param url the remote URL
     * @return this Builder
     */
    def remote(url: String): Builder =
      remoteUrl = Some(url)
      this

    /**
     * Sets the application name.
     *
     * @param name the application name
     * @return this Builder
     */
    def appName(name: String): Builder =
      appNameValue = Some(name)
      this

    /**
     * Sets a configuration property.
     *
     * @param key the configuration key
     * @param value the configuration value
     * @return this Builder
     */
    def config(key: String, value: String): Builder =
      configs = configs + (key -> value)
      this

    /**
     * Sets a configuration property with a numeric value.
     *
     * @param key the configuration key
     * @param value the configuration value
     * @return this Builder
     */
    def config(key: String, value: Long): Builder =
      config(key, value.toString)

    /**
     * Sets a configuration property with a boolean value.
     *
     * @param key the configuration key
     * @param value the configuration value
     * @return this Builder
     */
    def config(key: String, value: Boolean): Builder =
      config(key, value.toString)

    /**
     * Sets multiple configuration properties.
     *
     * @param configs a map of configuration key-value pairs
     * @return this Builder
     */
    def configs(configs: Map[String, String]): Builder =
      this.configs = this.configs ++ configs
      this

    /**
     * Sets the session ID. If not specified, a random UUID will be generated.
     *
     * @param sessionId the session ID
     * @return this Builder
     */
    def sessionId(sessionId: String): Builder =
      sessionIdValue = Some(sessionId)
      this

    /**
     * Builds and returns a new SparkSession.
     *
     * If no remote URL is specified via `remote()`, the builder will check the
     * `SPARK_REMOTE` environment variable. If that's not set, it defaults to
     * `sc://localhost:15002`.
     *
     * @return a Resource containing the SparkSession
     */
    def build(): Resource[IO, SparkSession] =
      val url = remoteUrl
        .orElse(Option(System.getenv("SPARK_REMOTE")))
        .getOrElse("sc://localhost:15002")
      val sessionId = sessionIdValue.getOrElse(UUID.randomUUID().toString.toLowerCase)

      val allConfigs = appNameValue match
        case Some(name) => configs + ("spark.app.name" -> name)
        case None => configs

      Resource.make(
        SparkConnectClient.create(url, sessionId, allConfigs)
          .map(client => new SparkSession(client, sessionId))
      )(session => session.stop())

    /**
     * Gets or creates a SparkSession.
     * If a session already exists with the same configuration, returns that session.
     * Otherwise, creates a new session.
     *
     * @return a Resource containing the SparkSession
     */
    def getOrCreate(): Resource[IO, SparkSession] =
      // For now, just create a new session
      // TODO: Implement session caching/reuse logic
      build()
