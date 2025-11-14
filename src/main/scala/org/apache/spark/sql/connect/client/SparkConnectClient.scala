package org.apache.spark.sql.connect.client

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import io.grpc.{ManagedChannel, ManagedChannelBuilder, Metadata}
import io.grpc.stub.MetadataUtils
import com.google.protobuf.ByteString
import scala.jdk.CollectionConverters.*
import java.net.URI
import java.util.concurrent.TimeUnit

// Import generated protobuf classes
import org.apache.spark.connect.proto.base.*
import org.apache.spark.connect.proto.relations.*
import org.apache.spark.connect.proto.commands.*
import org.apache.spark.connect.proto.common.*

/**
 * SparkConnectClient handles the gRPC communication with a Spark Connect server.
 *
 * This client provides low-level access to the Spark Connect protocol,
 * wrapping gRPC calls with functional effect types (IO).
 *
 * @param channel the gRPC channel
 * @param stub the SparkConnectService stub
 * @param sessionId the session ID
 * @param userId the user ID
 */
final class SparkConnectClient private (
  private val channel: ManagedChannel,
  private val stub: SparkConnectServiceGrpc.SparkConnectServiceBlockingStub,
  val sessionId: String,
  val userId: String
):

  /**
   * Execute a plan and stream results back.
   *
   * @param plan the execution plan
   * @return an iterator of responses
   */
  def executePlan(plan: Plan): IO[Iterator[ExecutePlanResponse]] =
    IO {
      val request = ExecutePlanRequest(
        sessionId = sessionId,
        userContext = Some(UserContext(userId = userId)),
        plan = Some(plan)
      )
      stub.executePlan(request)
    }

  /**
   * Execute a command.
   *
   * @param command the command to execute
   * @return an IO effect
   */
  def executeCommand(command: Command): IO[Unit] =
    IO {
      val plan = Plan(
        opType = Plan.OpType.Command(command)
      )
      val request = ExecutePlanRequest(
        sessionId = sessionId,
        userContext = Some(UserContext(userId = userId)),
        plan = Some(plan)
      )
      // Execute and consume all responses
      stub.executePlan(request).foreach(_ => ())
      ()
    }

  /**
   * Analyze a plan without executing it.
   *
   * @param plan the plan to analyze
   * @return the analysis response
   */
  def analyzePlan(plan: Plan): IO[AnalyzePlanResponse] =
    IO {
      val analyze = AnalyzePlanRequest.Analyze.Schema(
        AnalyzePlanRequest.Schema(plan = Some(plan))
      )
      val request = AnalyzePlanRequest(
        sessionId = sessionId,
        userContext = Some(UserContext(userId = userId)),
        analyze = analyze
      )
      stub.analyzePlan(request)
    }

  /**
   * Get the Spark version.
   *
   * @return the Spark version string
   */
  def version(): IO[String] =
    IO {
      val analyze = AnalyzePlanRequest.Analyze.SparkVersion(
        AnalyzePlanRequest.SparkVersion()
      )
      val request = AnalyzePlanRequest(
        sessionId = sessionId,
        userContext = Some(UserContext(userId = userId)),
        analyze = analyze
      )
      val response = stub.analyzePlan(request)
      response.result match
        case AnalyzePlanResponse.Result.SparkVersion(v) => v.version
        case _ => "unknown"
    }

  /**
   * Get a configuration value.
   *
   * @param key the configuration key
   * @return the configuration value
   */
  def getConfig(key: String): IO[String] =
    IO {
      val operation = ConfigRequest.Operation(
        opType = ConfigRequest.Operation.OpType.Get(
          ConfigRequest.Get(keys = Seq(key))
        )
      )
      val request = ConfigRequest(
        sessionId = sessionId,
        userContext = Some(UserContext(userId = userId)),
        operation = Some(operation)
      )
      val response = stub.config(request)
      response.pairs.headOption.flatMap(_.value).getOrElse("")
    }

  /**
   * Set a configuration value.
   *
   * @param key the configuration key
   * @param value the configuration value
   */
  def setConfig(key: String, value: String): IO[Unit] =
    IO {
      val operation = ConfigRequest.Operation(
        opType = ConfigRequest.Operation.OpType.Set(
          ConfigRequest.Set(
            pairs = Seq(KeyValue(key = key, value = Some(value)))
          )
        )
      )
      val request = ConfigRequest(
        sessionId = sessionId,
        userContext = Some(UserContext(userId = userId)),
        operation = Some(operation)
      )
      stub.config(request)
      ()
    }

  /**
   * Get all configuration values.
   *
   * @return a map of all configuration key-value pairs
   */
  def getAllConfig: IO[Map[String, String]] =
    IO {
      val operation = ConfigRequest.Operation(
        opType = ConfigRequest.Operation.OpType.GetAll(
          ConfigRequest.GetAll()
        )
      )
      val request = ConfigRequest(
        sessionId = sessionId,
        userContext = Some(UserContext(userId = userId)),
        operation = Some(operation)
      )
      val response = stub.config(request)
      response.pairs.map(p => p.key -> p.value.getOrElse("")).toMap
    }

  /**
   * Execute a SQL command.
   *
   * @param sql the SQL string
   * @return the relation representing the SQL result
   */
  def sql(sql: String): IO[Relation] =
    IO.pure {
      Relation(
        common = Some(RelationCommon(planId = Some(newPlanId()))),
        relType = Relation.RelType.Sql(
          SQL(query = sql)
        )
      )
    }

  /**
   * Create a relation for reading a table.
   *
   * @param tableName the table name
   * @return the relation representing the table
   */
  def readTable(tableName: String): IO[Relation] =
    IO.pure {
      Relation(
        common = Some(RelationCommon(planId = Some(newPlanId()))),
        relType = Relation.RelType.Read(
          Read(
            readType = Read.ReadType.NamedTable(
              Read.NamedTable(unparsedIdentifier = tableName)
            )
          )
        )
      )
    }

  /**
   * Create a range relation.
   *
   * @param start the start value
   * @param end the end value
   * @param step the step value
   * @param numPartitions the number of partitions
   * @return the range relation
   */
  def range(start: Long, end: Long, step: Long, numPartitions: Option[Int]): IO[Relation] =
    IO.pure {
      Relation(
        common = Some(RelationCommon(planId = Some(newPlanId()))),
        relType = Relation.RelType.Range(
          Range(
            start = Some(start),
            end = end,
            step = step,
            numPartitions = numPartitions
          )
        )
      )
    }

  /**
   * Close the client and release resources.
   */
  def close(): IO[Unit] =
    IO {
      channel.shutdown()
      channel.awaitTermination(5, TimeUnit.SECONDS)
      ()
    }

  /**
   * Generate a new unique plan ID.
   */
  private def newPlanId(): Long =
    System.nanoTime()

object SparkConnectClient:

  /**
   * Parse a Spark Connect URL and extract connection parameters.
   *
   * Format: sc://host:port[;param=value]
   *
   * @param url the connection URL
   * @return a tuple of (host, port, parameters)
   */
  private def parseUrl(url: String): IO[(String, Int, Map[String, String])] =
    IO {
      val uri = new URI(url.replaceFirst("sc://", "http://"))
      require(uri.getScheme == "http", "URL must start with sc://")

      val host = uri.getHost
      val port = if uri.getPort > 0 then uri.getPort else 15002

      val params = Option(uri.getQuery)
        .map(_.split(";").toSeq)
        .getOrElse(Seq.empty)
        .flatMap { param =>
          param.split("=", 2) match
            case Array(key, value) => Some(key -> value)
            case _ => None
        }
        .toMap

      (host, port, params)
    }

  /**
   * Create a new SparkConnectClient.
   *
   * @param url the Spark Connect URL
   * @param sessionId the session ID
   * @param configs additional configuration
   * @return an IO containing the client
   */
  def create(
    url: String,
    sessionId: String,
    configs: Map[String, String] = Map.empty
  ): IO[SparkConnectClient] =
    parseUrl(url).flatMap { case (host, port, params) =>
      val userId = params.getOrElse("user_id", System.getProperty("user.name", "anonymous"))
      val token = params.get("token").orElse(Option(System.getenv("SPARK_CONNECT_AUTHENTICATE_TOKEN")))
      val useSsl = params.get("use_ssl").exists(_.toLowerCase == "true")

      val channelBuilder = ManagedChannelBuilder
        .forAddress(host, port)
        .userAgent(s"spark-connect-scala/0.1.0")

      val channel = if useSsl then
        channelBuilder.useTransportSecurity().build()
      else
        channelBuilder.usePlaintext().build()

      val stub = token match
        case Some(t) =>
          val metadata = new Metadata()
          val key = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)
          metadata.put(key, s"Bearer $t")
          SparkConnectServiceGrpc.blockingStub(channel)
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
        case None =>
          SparkConnectServiceGrpc.blockingStub(channel)

      val client = new SparkConnectClient(channel, stub, sessionId, userId)

      // Apply configurations
      configs.toSeq.traverse { case (key, value) =>
        client.setConfig(key, value)
      }.map(_ => client)
    }

  /**
   * Create a SparkConnectClient as a Resource.
   *
   * @param url the Spark Connect URL
   * @param sessionId the session ID
   * @param configs additional configuration
   * @return a Resource containing the client
   */
  def resource(
    url: String,
    sessionId: String,
    configs: Map[String, String] = Map.empty
  ): Resource[IO, SparkConnectClient] =
    Resource.make(create(url, sessionId, configs))(_.close())
