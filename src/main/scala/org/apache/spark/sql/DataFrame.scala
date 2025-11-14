package org.apache.spark.sql

import cats.effect.IO
import cats.syntax.all.*
import org.apache.spark.sql.connect.client.SparkConnectClient
import org.apache.spark.connect.proto.base.*
import org.apache.spark.connect.proto.relations.*
import org.apache.spark.connect.proto.expressions.Expression
import scala.collection.mutable

/**
 * A distributed collection of data organized into named columns.
 *
 * DataFrame provides a domain-specific language for structured data manipulation.
 * Operations on DataFrames are lazily evaluated and form a logical execution plan
 * that is sent to the Spark Connect server for execution.
 *
 * DataFrames are immutable - all transformations return a new DataFrame.
 */
final class DataFrame private (
  private[sql] val session: SparkSession,
  private[sql] val relation: Relation
):

  private def client: SparkConnectClient = session.client

  // ============================================================================
  // Transformations - Return new DataFrames
  // ============================================================================

  /**
   * Select specific columns.
   *
   * @param cols the columns to select
   * @return a new DataFrame with selected columns
   */
  def select(cols: Column*): DataFrame =
    val projectRelation = Relation(
      common = Some(RelationCommon(planId = Some(newPlanId()))),
      relType = Relation.RelType.Project(
        Project(
          input = Some(relation),
          expressions = cols.map(_.expr).toSeq
        )
      )
    )
    DataFrame(session, projectRelation)

  /**
   * Select columns by name.
   *
   * @param colNames the column names to select
   * @return a new DataFrame with selected columns
   */
  @scala.annotation.targetName("selectByName")
  def select(colNames: String*): DataFrame =
    select(colNames.map(Column(_))*)

  /**
   * Filter rows using a condition.
   *
   * @param condition the filter condition
   * @return a new DataFrame with filtered rows
   */
  def filter(condition: Column): DataFrame =
    val filterRelation = Relation(
      common = Some(RelationCommon(planId = Some(newPlanId()))),
      relType = Relation.RelType.Filter(
        Filter(
          input = Some(relation),
          condition = Some(condition.expr)
        )
      )
    )
    DataFrame(session, filterRelation)

  /**
   * Filter rows using a condition (alias for filter).
   *
   * @param condition the filter condition
   * @return a new DataFrame with filtered rows
   */
  def where(condition: Column): DataFrame =
    filter(condition)

  /**
   * Filter rows using a SQL expression.
   *
   * @param conditionExpr the filter condition as a SQL expression
   * @return a new DataFrame with filtered rows
   */
  def where(conditionExpr: String): DataFrame =
    filter(functions.expr(conditionExpr))

  /**
   * Group by specified columns for aggregation.
   *
   * @param cols the columns to group by
   * @return a GroupedData for aggregation operations
   */
  def groupBy(cols: Column*): GroupedData =
    GroupedData(this, cols.toSeq, GroupedData.GroupType.GroupBy)

  /**
   * Group by specified column names for aggregation.
   *
   * @param colNames the column names to group by
   * @return a GroupedData for aggregation operations
   */
  @scala.annotation.targetName("groupByName")
  def groupBy(colNames: String*): GroupedData =
    groupBy(colNames.map(Column(_))*)

  /**
   * Create a multi-dimensional rollup for the specified columns.
   *
   * @param cols the columns to rollup
   * @return a GroupedData for aggregation operations
   */
  def rollup(cols: Column*): GroupedData =
    GroupedData(this, cols.toSeq, GroupedData.GroupType.Rollup)

  /**
   * Create a multi-dimensional rollup for the specified column names.
   *
   * @param colNames the column names to rollup
   * @return a GroupedData for aggregation operations
   */
  @scala.annotation.targetName("rollupName")
  def rollup(colNames: String*): GroupedData =
    rollup(colNames.map(Column(_))*)

  /**
   * Create a multi-dimensional cube for the specified columns.
   *
   * @param cols the columns to cube
   * @return a GroupedData for aggregation operations
   */
  def cube(cols: Column*): GroupedData =
    GroupedData(this, cols.toSeq, GroupedData.GroupType.Cube)

  /**
   * Create a multi-dimensional cube for the specified column names.
   *
   * @param colNames the column names to cube
   * @return a GroupedData for aggregation operations
   */
  @scala.annotation.targetName("cubeName")
  def cube(colNames: String*): GroupedData =
    cube(colNames.map(Column(_))*)

  /**
   * Unpivot a DataFrame from wide format to long format.
   *
   * @param ids ID columns that will remain unchanged
   * @param values value columns to unpivot
   * @param variableColumnName name of the variable column
   * @param valueColumnName name of the value column
   * @return unpivoted DataFrame
   */
  def unpivot(
      ids: Array[Column],
      values: Array[Column],
      variableColumnName: String,
      valueColumnName: String
  ): DataFrame =
    val unpivotRelation = Relation(
      common = Some(RelationCommon(planId = Some(newPlanId()))),
      relType = Relation.RelType.Unpivot(
        Unpivot(
          input = Some(relation),
          ids = ids.map(_.expr).toSeq,
          values = Some(Unpivot.Values(values = values.map(_.expr).toSeq)),
          variableColumnName = variableColumnName,
          valueColumnName = valueColumnName
        )
      )
    )
    DataFrame(session, unpivotRelation)

  /**
   * Unpivot a DataFrame from wide format to long format.
   * All non-ID columns will be unpivoted.
   *
   * @param ids ID columns that will remain unchanged
   * @param variableColumnName name of the variable column
   * @param valueColumnName name of the value column
   * @return unpivoted DataFrame
   */
  def unpivot(
      ids: Array[Column],
      variableColumnName: String,
      valueColumnName: String
  ): DataFrame =
    val unpivotRelation = Relation(
      common = Some(RelationCommon(planId = Some(newPlanId()))),
      relType = Relation.RelType.Unpivot(
        Unpivot(
          input = Some(relation),
          ids = ids.map(_.expr).toSeq,
          values = None,
          variableColumnName = variableColumnName,
          valueColumnName = valueColumnName
        )
      )
    )
    DataFrame(session, unpivotRelation)

  /**
   * Sort the DataFrame by the specified columns.
   *
   * @param cols the columns to sort by
   * @return a new DataFrame sorted by the specified columns
   */
  def sort(cols: Column*): DataFrame =
    orderBy(cols*)

  /**
   * Sort the DataFrame by the specified columns.
   *
   * @param cols the columns to sort by
   * @return a new DataFrame sorted by the specified columns
   */
  def orderBy(cols: Column*): DataFrame =
    val sortRelation = Relation(
      common = Some(RelationCommon(planId = Some(newPlanId()))),
      relType = Relation.RelType.Sort(
        Sort(
          input = Some(relation),
          order = cols.map(_.toSortOrder).toSeq,
          isGlobal = Some(true)
        )
      )
    )
    DataFrame(session, sortRelation)

  /**
   * Limit the number of rows.
   *
   * @param n the number of rows to limit to
   * @return a new DataFrame with at most n rows
   */
  def limit(n: Int): DataFrame =
    val limitRelation = Relation(
      common = Some(RelationCommon(planId = Some(newPlanId()))),
      relType = Relation.RelType.Limit(
        Limit(
          input = Some(relation),
          limit = n
        )
      )
    )
    DataFrame(session, limitRelation)

  /**
   * Add a new column or replace an existing column.
   *
   * @param colName the column name
   * @param col the column expression
   * @return a new DataFrame with the column added or replaced
   */
  def withColumn(colName: String, col: Column): DataFrame =
    val aliasedCol = col.alias(colName)
    val projectRelation = Relation(
      common = Some(RelationCommon(planId = Some(newPlanId()))),
      relType = Relation.RelType.WithColumns(
        WithColumns(
          input = Some(relation),
          aliases = Seq(Expression.Alias(
            expr = Some(col.expr),
            name = Seq(colName)
          ))
        )
      )
    )
    DataFrame(session, projectRelation)

  /**
   * Rename a column.
   *
   * @param existingName the existing column name
   * @param newName the new column name
   * @return a new DataFrame with the column renamed
   */
  def withColumnRenamed(existingName: String, newName: String): DataFrame =
    withColumn(newName, Column(existingName))

  /**
   * Drop specified columns.
   *
   * @param colNames the column names to drop
   * @return a new DataFrame without the specified columns
   */
  def drop(colNames: String*): DataFrame =
    val dropRelation = Relation(
      common = Some(RelationCommon(planId = Some(newPlanId()))),
      relType = Relation.RelType.Drop(
        Drop(
          input = Some(relation),
          columns = colNames.map { name =>
            Expression(
              exprType = Expression.ExprType.UnresolvedAttribute(
                Expression.UnresolvedAttribute(unparsedIdentifier = name)
              )
            )
          }.toSeq
        )
      )
    )
    DataFrame(session, dropRelation)

  /**
   * Remove duplicate rows.
   *
   * @return a new DataFrame without duplicate rows
   */
  def distinct(): DataFrame =
    dropDuplicates()

  /**
   * Remove duplicate rows, optionally considering only specific columns.
   *
   * @param colNames the column names to consider for duplicates
   * @return a new DataFrame without duplicate rows
   */
  def dropDuplicates(colNames: String*): DataFrame =
    val dedupRelation = Relation(
      common = Some(RelationCommon(planId = Some(newPlanId()))),
      relType = Relation.RelType.Deduplicate(
        Deduplicate(
          input = Some(relation),
          columnNames = colNames.toSeq
        )
      )
    )
    DataFrame(session, dedupRelation)

  /**
   * Join with another DataFrame.
   *
   * @param right the right DataFrame to join with
   * @param joinExprs the join condition
   * @param joinType the join type (inner, left, right, full, cross)
   * @return a new DataFrame with the join result
   */
  def join(right: DataFrame, joinExprs: Column, joinType: String = "inner"): DataFrame =
    val joinRelation = Relation(
      common = Some(RelationCommon(planId = Some(newPlanId()))),
      relType = Relation.RelType.Join(
        Join(
          left = Some(relation),
          right = Some(right.relation),
          joinCondition = Some(joinExprs.expr),
          joinType = joinTypeToProto(joinType)
        )
      )
    )
    DataFrame(session, joinRelation)

  /**
   * Cross join with another DataFrame.
   *
   * @param right the right DataFrame to cross join with
   * @return a new DataFrame with the cross join result
   */
  def crossJoin(right: DataFrame): DataFrame =
    val joinRelation = Relation(
      common = Some(RelationCommon(planId = Some(newPlanId()))),
      relType = Relation.RelType.Join(
        Join(
          left = Some(relation),
          right = Some(right.relation),
          joinType = Join.JoinType.JOIN_TYPE_CROSS
        )
      )
    )
    DataFrame(session, joinRelation)

  /**
   * Union with another DataFrame.
   *
   * @param other the other DataFrame to union with
   * @return a new DataFrame with the union result
   */
  def union(other: DataFrame): DataFrame =
    setOperation(other, SetOperation.SetOpType.SET_OP_TYPE_UNION, byName = false, allowMissingColumns = false)

  /**
   * Union with another DataFrame, matching columns by name.
   *
   * @param other the other DataFrame to union with
   * @param allowMissingColumns whether to allow missing columns
   * @return a new DataFrame with the union result
   */
  def unionByName(other: DataFrame, allowMissingColumns: Boolean = false): DataFrame =
    setOperation(other, SetOperation.SetOpType.SET_OP_TYPE_UNION, byName = true, allowMissingColumns)

  /**
   * Intersect with another DataFrame.
   *
   * @param other the other DataFrame to intersect with
   * @return a new DataFrame with the intersection result
   */
  def intersect(other: DataFrame): DataFrame =
    setOperation(other, SetOperation.SetOpType.SET_OP_TYPE_INTERSECT, byName = false, allowMissingColumns = false)

  /**
   * Except with another DataFrame.
   *
   * @param other the other DataFrame to except with
   * @return a new DataFrame with the except result
   */
  def except(other: DataFrame): DataFrame =
    setOperation(other, SetOperation.SetOpType.SET_OP_TYPE_EXCEPT, byName = false, allowMissingColumns = false)

  private def setOperation(
    other: DataFrame,
    opType: SetOperation.SetOpType,
    byName: Boolean,
    allowMissingColumns: Boolean
  ): DataFrame =
    val setOpRelation = Relation(
      common = Some(RelationCommon(planId = Some(newPlanId()))),
      relType = Relation.RelType.SetOp(
        SetOperation(
          leftInput = Some(relation),
          rightInput = Some(other.relation),
          setOpType = opType,
          byName = Some(byName),
          allowMissingColumns = Some(allowMissingColumns)
        )
      )
    )
    DataFrame(session, setOpRelation)

  // ============================================================================
  // Actions - Trigger execution and return results
  // ============================================================================

  /**
   * Count the number of rows.
   *
   * @return the number of rows
   */
  def count(): IO[Long] =
    import functions.count as countFn
    groupBy(Seq[Column]():_*).agg(countFn(functions.lit(1)).as("count"))
      .collect()
      .map(_.head.getLong(0))

  /**
   * Collect all rows to the client.
   *
   * @return a sequence of all rows
   */
  def collect(): IO[Seq[Row]] =
    val plan = Plan(
      opType = Plan.OpType.Root(relation)
    )
    client.executePlan(plan).flatMap { responses =>
      IO {
        val rows = mutable.ArrayBuffer[Row]()
        responses.foreach { response =>
          response.responseType match
            case ExecutePlanResponse.ResponseType.ArrowBatch(arrowBatch) =>
              // Parse Arrow batch and convert to Row objects
              val batchRows = org.apache.spark.sql.connect.client.ArrowConverter
                .arrowBatchToRows(arrowBatch.data.toByteArray)
              rows ++= batchRows
            case _ =>
              // Skip non-data responses (metrics, schema, etc.)
        }
        rows.toSeq
      }
    }

  /**
   * Show the first n rows in tabular form.
   *
   * @param numRows the number of rows to show
   * @param truncate whether to truncate long values
   * @return an IO effect
   */
  def show(numRows: Int = 20, truncate: Boolean = true): IO[Unit] =
    limit(numRows).collect().map { rows =>
      if rows.isEmpty then
        println(s"(0 rows)")
      else
        // Format as a simple table
        val maxWidth = if truncate then 20 else Int.MaxValue

        rows.zipWithIndex.foreach { case (row, idx) =>
          val values = row.toSeq.map { value =>
            val str = if value == null then "null" else value.toString
            if str.length > maxWidth then str.take(maxWidth - 3) + "..." else str
          }
          println(s"Row $idx: ${values.mkString(", ")}")
        }

        println(s"\n(${ rows.size} row${if rows.size != 1 then "s" else ""})")
    }

  /**
   * Return the first row.
   *
   * @return the first row
   */
  def first(): IO[Row] =
    limit(1).collect().map(_.head)

  /**
   * Return the first n rows.
   *
   * @param n the number of rows
   * @return the first n rows
   */
  def head(n: Int): IO[Seq[Row]] =
    limit(n).collect()

  /**
   * Return the first n rows.
   *
   * @param n the number of rows
   * @return the first n rows
   */
  def take(n: Int): IO[Seq[Row]] =
    head(n)

  /**
   * Print the schema in a tree format.
   *
   * @return an IO effect
   */
  def printSchema(): IO[Unit] =
    schema.map { s =>
      println("root")
      // TODO: Format schema nicely
      println(s.toString)
    }

  /**
   * Get the schema of the DataFrame.
   *
   * @return the schema
   */
  def schema: IO[StructType] =
    val plan = Plan(
      opType = Plan.OpType.Root(relation)
    )
    client.analyzePlan(plan).map { response =>
      response.result match
        case AnalyzePlanResponse.Result.Schema(schemaResult) =>
          schemaResult.schema match
            case Some(protoDataType) =>
              DataTypeConverter.fromProto(protoDataType) match
                case st: StructType => st
                case _ => StructType(Seq.empty) // Fallback if not a struct
            case None => StructType(Seq.empty)
        case _ =>
          StructType(Seq.empty)
    }

  /**
   * Get the column names of the DataFrame.
   *
   * @return array of column names
   */
  def columns: IO[Array[String]] =
    schema.map(_.fields.map(_.name).toArray)

  /**
   * Explain the physical plan.
   *
   * @param extended whether to show extended information
   * @return an IO effect
   */
  def explain(extended: Boolean = false): IO[Unit] =
    // For now, just print a placeholder
    // TODO: Implement proper explain using analyzePlan
    IO.println(s"DataFrame explain (extended=$extended) - not yet fully implemented")

  /**
   * Repartition the DataFrame to the specified number of partitions.
   *
   * @param numPartitions the target number of partitions
   * @return a new DataFrame with the specified number of partitions
   */
  def repartition(numPartitions: Int): DataFrame =
    val repartitionRelation = Relation(
      common = Some(RelationCommon(planId = Some(newPlanId()))),
      relType = Relation.RelType.Repartition(
        Repartition(
          input = Some(relation),
          numPartitions = numPartitions,
          shuffle = Some(true)
        )
      )
    )
    DataFrame(session, repartitionRelation)

  /**
   * Repartition the DataFrame by the specified columns.
   *
   * @param cols the columns to repartition by
   * @return a new DataFrame repartitioned by the columns
   */
  def repartition(cols: Column*): DataFrame =
    val repartitionRelation = Relation(
      common = Some(RelationCommon(planId = Some(newPlanId()))),
      relType = Relation.RelType.RepartitionByExpression(
        RepartitionByExpression(
          input = Some(relation),
          partitionExprs = cols.map(_.expr).toSeq
        )
      )
    )
    DataFrame(session, repartitionRelation)

  /**
   * Repartition the DataFrame by columns with a target number of partitions.
   *
   * @param numPartitions the target number of partitions
   * @param cols the columns to repartition by
   * @return a new DataFrame
   */
  def repartition(numPartitions: Int, cols: Column*): DataFrame =
    val repartitionRelation = Relation(
      common = Some(RelationCommon(planId = Some(newPlanId()))),
      relType = Relation.RelType.RepartitionByExpression(
        RepartitionByExpression(
          input = Some(relation),
          numPartitions = Some(numPartitions),
          partitionExprs = cols.map(_.expr).toSeq
        )
      )
    )
    DataFrame(session, repartitionRelation)

  /**
   * Coalesce the DataFrame to the specified number of partitions.
   * This is a narrow operation that does not trigger a shuffle.
   *
   * @param numPartitions the target number of partitions
   * @return a new DataFrame with the specified number of partitions
   */
  def coalesce(numPartitions: Int): DataFrame =
    val repartitionRelation = Relation(
      common = Some(RelationCommon(planId = Some(newPlanId()))),
      relType = Relation.RelType.Repartition(
        Repartition(
          input = Some(relation),
          numPartitions = numPartitions,
          shuffle = Some(false)  // No shuffle for coalesce
        )
      )
    )
    DataFrame(session, repartitionRelation)

  /**
   * Generate summary statistics for the DataFrame.
   *
   * @param cols the columns to describe
   * @return a DataFrame with summary statistics
   */
  def describe(cols: String*): DataFrame =
    val describeRelation = Relation(
      common = Some(RelationCommon(planId = Some(newPlanId()))),
      relType = Relation.RelType.Describe(
        StatDescribe(
          input = Some(relation),
          cols = cols.toSeq
        )
      )
    )
    DataFrame(session, describeRelation)

  /**
   * Generate custom summary statistics.
   *
   * @param statistics the statistics to compute (e.g., "count", "mean", "stddev")
   * @return a DataFrame with summary statistics
   */
  def summary(statistics: String*): DataFrame =
    val summaryRelation = Relation(
      common = Some(RelationCommon(planId = Some(newPlanId()))),
      relType = Relation.RelType.Summary(
        StatSummary(
          input = Some(relation),
          statistics = statistics.toSeq
        )
      )
    )
    DataFrame(session, summaryRelation)

  /**
   * Sample a fraction of rows.
   *
   * @param withReplacement whether to sample with replacement
   * @param fraction the fraction of rows to return
   * @param seed the random seed
   * @return a new DataFrame with sampled rows
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long = 0L): DataFrame =
    val sampleRelation = Relation(
      common = Some(RelationCommon(planId = Some(newPlanId()))),
      relType = Relation.RelType.Sample(
        Sample(
          input = Some(relation),
          lowerBound = 0.0,
          upperBound = fraction,
          withReplacement = Some(withReplacement),
          seed = Some(seed)
        )
      )
    )
    DataFrame(session, sampleRelation)

  /**
   * Sample a fraction of rows (without replacement).
   *
   * @param fraction the fraction of rows to return
   * @return a new DataFrame with sampled rows
   */
  def sample(fraction: Double): DataFrame =
    sample(withReplacement = false, fraction)

  /**
   * Randomly split the DataFrame into multiple DataFrames.
   *
   * @param weights the weights for each split
   * @param seed the random seed
   * @return an array of DataFrames
   */
  def randomSplit(weights: Array[Double], seed: Long = 0L): Array[DataFrame] =
    val sum = weights.sum
    val normalizedWeights = weights.map(_ / sum)
    val cumulative = normalizedWeights.scanLeft(0.0)(_ + _).toSeq

    cumulative.sliding(2).map { case Seq(lower, upper) =>
      val sampleRelation = Relation(
        common = Some(RelationCommon(planId = Some(newPlanId()))),
        relType = Relation.RelType.Sample(
          Sample(
            input = Some(relation),
            lowerBound = lower,
            upperBound = upper,
            withReplacement = Some(false),
            seed = Some(seed)
          )
        )
      )
      DataFrame(session, sampleRelation)
    }.toArray

  /**
   * Cache the DataFrame in memory.
   *
   * @return this DataFrame
   */
  def cache(): DataFrame =
    persist()

  /**
   * Persist the DataFrame with the default storage level.
   *
   * @return this DataFrame
   */
  def persist(): DataFrame =
    // TODO: Implement persist via Catalog API
    this

  /**
   * Unpersist the DataFrame.
   *
   * @param blocking whether to block until unpersist completes
   * @return this DataFrame
   */
  def unpersist(blocking: Boolean = false): DataFrame =
    // TODO: Implement unpersist via Catalog API
    this

  /**
   * Get access to DataFrameNaFunctions for handling null/NaN values.
   *
   * @return DataFrameNaFunctions
   */
  def na: DataFrameNaFunctions =
    DataFrameNaFunctions(this)

  /**
   * Get access to DataFrameStatFunctions for statistical operations.
   *
   * @return DataFrameStatFunctions
   */
  def stat: DataFrameStatFunctions =
    DataFrameStatFunctions(this)

  /**
   * Get a DataFrameWriter for writing this DataFrame.
   *
   * @return a DataFrameWriter
   */
  def write: DataFrameWriter =
    DataFrameWriter(this)

  // ============================================================================
  // Helper methods
  // ============================================================================

  private[sql] def newPlanId(): Long =
    System.nanoTime()

  private def joinTypeToProto(joinType: String): Join.JoinType =
    joinType.toLowerCase match
      case "inner" => Join.JoinType.JOIN_TYPE_INNER
      case "left" | "leftouter" => Join.JoinType.JOIN_TYPE_LEFT_OUTER
      case "right" | "rightouter" => Join.JoinType.JOIN_TYPE_RIGHT_OUTER
      case "full" | "outer" | "fullouter" => Join.JoinType.JOIN_TYPE_FULL_OUTER
      case "cross" => Join.JoinType.JOIN_TYPE_CROSS
      case "semi" | "leftsemi" => Join.JoinType.JOIN_TYPE_LEFT_SEMI
      case "anti" | "leftanti" => Join.JoinType.JOIN_TYPE_LEFT_ANTI
      case _ => Join.JoinType.JOIN_TYPE_INNER

object DataFrame:

  private[sql] def apply(session: SparkSession, relation: Relation): DataFrame =
    new DataFrame(session, relation)

  /**
   * Create a DataFrame from a SQL query.
   */
  private[sql] def sql(session: SparkSession, sqlText: String): DataFrame =
    val relation = Relation(
      common = Some(RelationCommon(planId = Some(System.nanoTime()))),
      relType = Relation.RelType.Sql(
        SQL(query = sqlText)
      )
    )
    DataFrame(session, relation)

  /**
   * Create a DataFrame from a table.
   */
  private[sql] def table(session: SparkSession, tableName: String): DataFrame =
    val relation = Relation(
      common = Some(RelationCommon(planId = Some(System.nanoTime()))),
      relType = Relation.RelType.Read(
        Read(
          readType = Read.ReadType.NamedTable(
            Read.NamedTable(unparsedIdentifier = tableName)
          )
        )
      )
    )
    DataFrame(session, relation)

  /**
   * Create a DataFrame with a range of values.
   */
  private[sql] def range(
    session: SparkSession,
    start: Long,
    end: Long,
    step: Long,
    numPartitions: Option[Int]
  ): DataFrame =
    val relation = Relation(
      common = Some(RelationCommon(planId = Some(System.nanoTime()))),
      relType = Relation.RelType.Range(
        Range(
          start = Some(start),
          end = end,
          step = step,
          numPartitions = numPartitions
        )
      )
    )
    DataFrame(session, relation)
