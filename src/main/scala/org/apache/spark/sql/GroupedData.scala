package org.apache.spark.sql

import org.apache.spark.connect.proto.relations.{Relation, RelationCommon, Aggregate}
import org.apache.spark.connect.proto.expressions.Expression

/**
 * A set of methods for aggregations on a DataFrame, created by groupBy or rollup or cube.
 *
 * The available aggregate methods are defined in this class.
 */
final class GroupedData private[sql] (
  private val df: DataFrame,
  private val groupingExprs: Seq[Column],
  private val groupType: GroupedData.GroupType,
  private val pivotCol: Option[Aggregate.Pivot] = None
):

  /**
   * Compute aggregates by specifying column expressions.
   *
   * @param exprs the aggregate expressions
   * @return a new DataFrame with the aggregation result
   */
  def agg(exprs: Column*): DataFrame =
    val aggregateBuilder = Aggregate(
      input = Some(df.relation),
      groupType = groupType.toProto,
      groupingExpressions = groupingExprs.map(_.expr).toSeq,
      aggregateExpressions = exprs.map(_.expr).toSeq,
      pivot = pivotCol
    )
    val relation = Relation(
      common = Some(RelationCommon(planId = Some(System.nanoTime()))),
      relType = Relation.RelType.Aggregate(aggregateBuilder)
    )
    DataFrame(df.session, relation)

  /**
   * Compute aggregates by specifying a map from column name to aggregate function.
   *
   * @param aggExprs the map from column name to aggregate function name
   * @return a new DataFrame with the aggregation result
   */
  def agg(aggExprs: Map[String, String]): DataFrame =
    val exprs = aggExprs.map { case (colName, aggFunc) =>
      aggFunc.toLowerCase match
        case "count" => functions.count(Column(colName))
        case "sum" => functions.sum(Column(colName))
        case "avg" | "mean" => functions.avg(Column(colName))
        case "max" => functions.max(Column(colName))
        case "min" => functions.min(Column(colName))
        case "first" => functions.first(Column(colName))
        case "last" => functions.last(Column(colName))
        case _ => throw new IllegalArgumentException(s"Unknown aggregate function: $aggFunc")
    }.toSeq
    agg(exprs*)

  /**
   * Count the number of rows in each group.
   *
   * @return a new DataFrame with the count
   */
  def count(): DataFrame =
    agg(functions.count(functions.lit(1)).as("count"))

  /**
   * Compute the sum for each numeric column in each group.
   *
   * @param colNames the column names to sum
   * @return a new DataFrame with the sum
   */
  def sum(colNames: String*): DataFrame =
    agg(colNames.map(name => functions.sum(Column(name)).as(s"sum($name)"))*)

  /**
   * Compute the average for each numeric column in each group.
   *
   * @param colNames the column names to average
   * @return a new DataFrame with the average
   */
  def avg(colNames: String*): DataFrame =
    agg(colNames.map(name => functions.avg(Column(name)).as(s"avg($name)"))*)

  /**
   * Compute the mean for each numeric column in each group (alias for avg).
   *
   * @param colNames the column names to compute mean for
   * @return a new DataFrame with the mean
   */
  def mean(colNames: String*): DataFrame =
    avg(colNames*)

  /**
   * Compute the max for each column in each group.
   *
   * @param colNames the column names to compute max for
   * @return a new DataFrame with the max
   */
  def max(colNames: String*): DataFrame =
    agg(colNames.map(name => functions.max(Column(name)).as(s"max($name)"))*)

  /**
   * Compute the min for each column in each group.
   *
   * @param colNames the column names to compute min for
   * @return a new DataFrame with the min
   */
  def min(colNames: String*): DataFrame =
    agg(colNames.map(name => functions.min(Column(name)).as(s"min($name)"))*)

  /**
   * Pivots a column of the current DataFrame and performs the specified aggregation.
   *
   * This is only applicable for a GroupedData that was created by groupBy.
   * The pivot column values will become new columns in the output.
   *
   * @param pivotColumn the name of the column to pivot
   * @return a pivoted GroupedData
   */
  def pivot(pivotColumn: String): GroupedData =
    pivot(Column(pivotColumn))

  /**
   * Pivots a column of the current DataFrame and performs the specified aggregation.
   *
   * This is only applicable for a GroupedData that was created by groupBy.
   * The pivot column will only contain the specified values.
   *
   * @param pivotColumn the name of the column to pivot
   * @param values list of values that will be translated to columns
   * @return a pivoted GroupedData
   */
  def pivot(pivotColumn: String, values: Seq[Any]): GroupedData =
    pivot(Column(pivotColumn), values)

  /**
   * Pivots a column of the current DataFrame and performs the specified aggregation.
   *
   * This is only applicable for a GroupedData that was created by groupBy.
   * The pivot column values will become new columns in the output.
   *
   * @param pivotColumn the column to pivot
   * @return a pivoted GroupedData
   */
  def pivot(pivotColumn: Column): GroupedData =
    pivot(pivotColumn, Nil)

  /**
   * Pivots a column of the current DataFrame and performs the specified aggregation.
   *
   * This is only applicable for a GroupedData that was created by groupBy.
   * The pivot column will only contain the specified values.
   *
   * @param pivotColumn the column to pivot
   * @param values list of values that will be translated to columns
   * @return a pivoted GroupedData
   */
  def pivot(pivotColumn: Column, values: Seq[Any]): GroupedData =
    groupType match
      case GroupedData.GroupType.GroupBy =>
        val valueExprs = values.map {
          case c: Column =>
            // Extract literal from column if it's a literal
            c.expr.exprType match
              case Expression.ExprType.Literal(lit) => lit
              case _ => throw new IllegalArgumentException("values only accept literal Column")
          case v =>
            // Convert to literal
            val litCol = functions.lit(v)
            litCol.expr.exprType match
              case Expression.ExprType.Literal(lit) => lit
              case _ => throw new IllegalArgumentException("Failed to convert value to literal")
        }

        val pivotBuilder = Aggregate.Pivot(
          col = Some(pivotColumn.expr),
          values = valueExprs
        )

        new GroupedData(
          df,
          groupingExprs,
          GroupedData.GroupType.Pivot,
          Some(pivotBuilder)
        )
      case _ =>
        throw new UnsupportedOperationException("pivot is only supported after a groupBy")

object GroupedData:

  enum GroupType:
    case GroupBy
    case Rollup
    case Cube
    case Pivot

    def toProto: Aggregate.GroupType = this match
      case GroupBy => Aggregate.GroupType.GROUP_TYPE_GROUPBY
      case Rollup => Aggregate.GroupType.GROUP_TYPE_ROLLUP
      case Cube => Aggregate.GroupType.GROUP_TYPE_CUBE
      case Pivot => Aggregate.GroupType.GROUP_TYPE_PIVOT
