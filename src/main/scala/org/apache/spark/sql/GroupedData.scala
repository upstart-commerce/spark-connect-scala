package org.apache.spark.sql

import org.apache.spark.connect.proto.relations.{Relation, RelationCommon, Aggregate}

/**
 * A set of methods for aggregations on a DataFrame, created by groupBy or rollup or cube.
 *
 * The available aggregate methods are defined in this class.
 */
final class GroupedData private[sql] (
  private val df: DataFrame,
  private val groupingExprs: Seq[Column],
  private val groupType: GroupedData.GroupType
):

  /**
   * Compute aggregates by specifying column expressions.
   *
   * @param exprs the aggregate expressions
   * @return a new DataFrame with the aggregation result
   */
  def agg(exprs: Column*): DataFrame =
    val relation = Relation(
      common = Some(RelationCommon(planId = Some(System.nanoTime()))),
      relType = Relation.RelType.Aggregate(
        Aggregate(
          input = Some(df.relation),
          groupType = groupType.toProto,
          groupingExpressions = groupingExprs.map(_.expr).toSeq,
          aggregateExpressions = exprs.map(_.expr).toSeq
        )
      )
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

object GroupedData:

  enum GroupType:
    case GroupBy
    case Rollup
    case Cube

    def toProto: Aggregate.GroupType = this match
      case GroupBy => Aggregate.GroupType.GROUP_TYPE_GROUPBY
      case Rollup => Aggregate.GroupType.GROUP_TYPE_ROLLUP
      case Cube => Aggregate.GroupType.GROUP_TYPE_CUBE
