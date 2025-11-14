package org.apache.spark.sql

import org.apache.spark.connect.proto.expressions.Expression
import org.apache.spark.connect.proto.relations.{Relation, RelationCommon, NAFill, NADrop, NAReplace}
import scala.collection.mutable

/**
 * Functions for handling missing/null data in DataFrames.
 *
 * DataFrameNaFunctions provides methods to:
 * - Drop rows with null/NaN values
 * - Fill null/NaN values with specified values
 * - Replace specific values with new values
 *
 * Access via df.na
 */
final class DataFrameNaFunctions private[sql] (df: DataFrame):

  /**
   * Drop rows that contain any null or NaN values.
   *
   * @return DataFrame with rows containing any nulls dropped
   */
  def drop(): DataFrame =
    drop("any", Array.empty[String])

  /**
   * Drop rows based on how many nulls are allowed.
   *
   * @param how "any" to drop rows with any nulls, "all" to drop rows with all nulls
   * @return DataFrame with rows dropped based on criteria
   */
  def drop(how: String): DataFrame =
    drop(how, Array.empty[String])

  /**
   * Drop rows based on nulls in specific columns.
   *
   * @param cols column names to consider
   * @return DataFrame with rows dropped
   */
  def drop(cols: Array[String]): DataFrame =
    drop("any", cols)

  /**
   * Drop rows based on nulls in specific columns.
   *
   * @param cols column names to consider
   * @return DataFrame with rows dropped
   */
  def drop(cols: Seq[String]): DataFrame =
    drop("any", cols.toArray)

  /**
   * Drop rows based on how many nulls are allowed in specific columns.
   *
   * @param how "any" to drop rows with any nulls, "all" to drop rows with all nulls
   * @param cols column names to consider
   * @return DataFrame with rows dropped
   */
  def drop(how: String, cols: Array[String]): DataFrame =
    val minNonNulls = how.toLowerCase match {
      case "any" =>
        // For "any", if cols are specified, require all those cols to be non-null
        // Otherwise require at least 1 non-null in all columns
        if (cols.isEmpty) Some(1) else Some(cols.length)
      case "all" => Some(1)          // Drop only if all null (at least 1 must be non-null)
      case _ => Some(1)              // Default to "all"
    }

    val dropRelation = Relation(
      common = Some(RelationCommon(planId = Some(System.nanoTime()))),
      relType = Relation.RelType.DropNa(
        NADrop(
          input = Some(df.relation),
          cols = cols.toSeq,
          minNonNulls = minNonNulls
        )
      )
    )
    DataFrame(df.session, dropRelation)

  /**
   * Drop rows based on how many nulls are allowed in specific columns.
   *
   * @param how "any" to drop rows with any nulls, "all" to drop rows with all nulls
   * @param cols column names to consider
   * @return DataFrame with rows dropped
   */
  def drop(how: String, cols: Seq[String]): DataFrame =
    drop(how, cols.toArray)

  /**
   * Drop rows that have less than minNonNulls non-null values.
   *
   * @param minNonNulls minimum number of non-null values required
   * @return DataFrame with rows dropped
   */
  def drop(minNonNulls: Int): DataFrame =
    drop(minNonNulls, Array.empty[String])

  /**
   * Drop rows that have less than minNonNulls non-null values in specified columns.
   *
   * @param minNonNulls minimum number of non-null values required
   * @param cols column names to consider
   * @return DataFrame with rows dropped
   */
  def drop(minNonNulls: Int, cols: Array[String]): DataFrame =
    val dropRelation = Relation(
      common = Some(RelationCommon(planId = Some(System.nanoTime()))),
      relType = Relation.RelType.DropNa(
        NADrop(
          input = Some(df.relation),
          cols = cols.toSeq,
          minNonNulls = Some(minNonNulls)
        )
      )
    )
    DataFrame(df.session, dropRelation)

  /**
   * Drop rows that have less than minNonNulls non-null values in specified columns.
   *
   * @param minNonNulls minimum number of non-null values required
   * @param cols column names to consider
   * @return DataFrame with rows dropped
   */
  def drop(minNonNulls: Int, cols: Seq[String]): DataFrame =
    drop(minNonNulls, cols.toArray)

  /**
   * Fill null values with a single value for all columns.
   *
   * @param value the value to replace nulls with
   * @return DataFrame with nulls filled
   */
  def fill(value: Long): DataFrame =
    fill(value, Array.empty[String])

  /**
   * Fill null values with a single value for all columns.
   *
   * @param value the value to replace nulls with
   * @return DataFrame with nulls filled
   */
  def fill(value: Double): DataFrame =
    fill(value, Array.empty[String])

  /**
   * Fill null values with a single value for all columns.
   *
   * @param value the value to replace nulls with
   * @return DataFrame with nulls filled
   */
  def fill(value: String): DataFrame =
    fill(value, Array.empty[String])

  /**
   * Fill null values with a single value for all columns.
   *
   * @param value the value to replace nulls with
   * @return DataFrame with nulls filled
   */
  def fill(value: Boolean): DataFrame =
    fill(value, Array.empty[String])

  /**
   * Fill null values with a value for specific columns.
   *
   * @param value the value to replace nulls with
   * @param cols column names to fill
   * @return DataFrame with nulls filled
   */
  def fill(value: Long, cols: Array[String]): DataFrame =
    if (cols.isEmpty) {
      // When no columns specified, create a single-value fill for all columns
      fillImpl(Map.empty, Some(value))
    } else {
      fillImpl(Map(cols.map(_ -> value)*), None)
    }

  /**
   * Fill null values with a value for specific columns.
   *
   * @param value the value to replace nulls with
   * @param cols column names to fill
   * @return DataFrame with nulls filled
   */
  def fill(value: Long, cols: Seq[String]): DataFrame =
    fill(value, cols.toArray)

  /**
   * Fill null values with a value for specific columns.
   *
   * @param value the value to replace nulls with
   * @param cols column names to fill
   * @return DataFrame with nulls filled
   */
  def fill(value: Double, cols: Array[String]): DataFrame =
    if (cols.isEmpty) {
      fillImpl(Map.empty, Some(value))
    } else {
      fillImpl(Map(cols.map(_ -> value)*), None)
    }

  /**
   * Fill null values with a value for specific columns.
   *
   * @param value the value to replace nulls with
   * @param cols column names to fill
   * @return DataFrame with nulls filled
   */
  def fill(value: Double, cols: Seq[String]): DataFrame =
    fill(value, cols.toArray)

  /**
   * Fill null values with a value for specific columns.
   *
   * @param value the value to replace nulls with
   * @param cols column names to fill
   * @return DataFrame with nulls filled
   */
  def fill(value: String, cols: Array[String]): DataFrame =
    if (cols.isEmpty) {
      fillImpl(Map.empty, Some(value))
    } else {
      fillImpl(Map(cols.map(_ -> value)*), None)
    }

  /**
   * Fill null values with a value for specific columns.
   *
   * @param value the value to replace nulls with
   * @param cols column names to fill
   * @return DataFrame with nulls filled
   */
  def fill(value: String, cols: Seq[String]): DataFrame =
    fill(value, cols.toArray)

  /**
   * Fill null values with a value for specific columns.
   *
   * @param value the value to replace nulls with
   * @param cols column names to fill
   * @return DataFrame with nulls filled
   */
  def fill(value: Boolean, cols: Array[String]): DataFrame =
    if (cols.isEmpty) {
      fillImpl(Map.empty, Some(value))
    } else {
      fillImpl(Map(cols.map(_ -> value)*), None)
    }

  /**
   * Fill null values with a value for specific columns.
   *
   * @param value the value to replace nulls with
   * @param cols column names to fill
   * @return DataFrame with nulls filled
   */
  def fill(value: Boolean, cols: Seq[String]): DataFrame =
    fill(value, cols.toArray)

  /**
   * Fill null values with column-specific values.
   *
   * @param valueMap map from column name to fill value
   * @return DataFrame with nulls filled
   */
  def fill(valueMap: Map[String, Any]): DataFrame =
    fillImpl(valueMap, None)

  /**
   * Fill null values with column-specific values (Java Map).
   *
   * @param valueMap map from column name to fill value
   * @return DataFrame with nulls filled
   */
  def fill(valueMap: java.util.Map[String, Any]): DataFrame =
    import scala.jdk.CollectionConverters._
    fill(valueMap.asScala.toMap)

  /**
   * Replace values matching keys with corresponding values.
   *
   * @param replacement map from old value to new value
   * @return DataFrame with values replaced
   */
  def replace[T](col: String, replacement: Map[T, T]): DataFrame =
    replace(Array(col), replacement)

  /**
   * Replace values matching keys with corresponding values in specific columns.
   *
   * @param cols column names to apply replacement
   * @param replacement map from old value to new value
   * @return DataFrame with values replaced
   */
  def replace[T](cols: Array[String], replacement: Map[T, T]): DataFrame =
    val replaceRelation = Relation(
      common = Some(RelationCommon(planId = Some(System.nanoTime()))),
      relType = Relation.RelType.Replace(
        NAReplace(
          input = Some(df.relation),
          cols = cols.toSeq,
          replacements = replacement.map { case (k, v) =>
            NAReplace.Replacement(
              oldValue = Some(toLiteralValue(k)),
              newValue = Some(toLiteralValue(v))
            )
          }.toSeq
        )
      )
    )
    DataFrame(df.session, replaceRelation)

  /**
   * Replace values matching keys with corresponding values in specific columns.
   *
   * @param cols column names to apply replacement
   * @param replacement map from old value to new value
   * @return DataFrame with values replaced
   */
  def replace[T](cols: Seq[String], replacement: Map[T, T]): DataFrame =
    replace(cols.toArray, replacement)

  /**
   * Replace values matching keys with corresponding values in all columns.
   *
   * @param replacement map from old value to new value
   * @return DataFrame with values replaced
   */
  def replace[T](replacement: Map[T, T]): DataFrame =
    replace(Array.empty[String], replacement)

  // Helper methods

  private def fillImpl(valueMap: Map[String, Any], defaultValue: Option[Any]): DataFrame =
    val (cols, values) = if (valueMap.isEmpty && defaultValue.isDefined) {
      // When no columns specified, use empty cols and single value for all columns
      (Seq.empty[String], Seq(toLiteralValue(defaultValue.get)))
    } else {
      // When columns specified, use column names and their corresponding values
      (valueMap.keys.toSeq, valueMap.values.map(toLiteralValue).toSeq)
    }

    val fillRelation = Relation(
      common = Some(RelationCommon(planId = Some(System.nanoTime()))),
      relType = Relation.RelType.FillNa(
        NAFill(
          input = Some(df.relation),
          cols = cols,
          values = values
        )
      )
    )
    DataFrame(df.session, fillRelation)

  private def toLiteralValue(value: Any): Expression.Literal =
    value match {
      case null => Expression.Literal(literalType = Expression.Literal.LiteralType.Null(
        org.apache.spark.connect.proto.types.DataType(
          kind = org.apache.spark.connect.proto.types.DataType.Kind.Null(
            org.apache.spark.connect.proto.types.DataType.NULL()
          )
        )
      ))
      case l: Long => Expression.Literal(literalType = Expression.Literal.LiteralType.Long(l))
      case i: Int => Expression.Literal(literalType = Expression.Literal.LiteralType.Long(i.toLong))
      case d: Double => Expression.Literal(literalType = Expression.Literal.LiteralType.Double(d))
      case f: Float => Expression.Literal(literalType = Expression.Literal.LiteralType.Double(f.toDouble))
      case s: String => Expression.Literal(literalType = Expression.Literal.LiteralType.String(s))
      case b: Boolean => Expression.Literal(literalType = Expression.Literal.LiteralType.Boolean(b))
      case _ => Expression.Literal(literalType = Expression.Literal.LiteralType.String(value.toString))
    }

object DataFrameNaFunctions:
  private[sql] def apply(df: DataFrame): DataFrameNaFunctions =
    new DataFrameNaFunctions(df)
