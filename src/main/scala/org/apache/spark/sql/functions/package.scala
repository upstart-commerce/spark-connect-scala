package org.apache.spark.sql

import org.apache.spark.connect.proto.expressions.Expression
import org.apache.spark.connect.proto.types.{DataType as ProtoDataType}
import com.google.protobuf.ByteString
import java.sql.{Date, Timestamp}

/**
 * Functions for DataFrame operations.
 *
 * This package object provides built-in functions that can be used in DataFrame operations.
 */
package object functions:

  // ============================================================================
  // Column creation
  // ============================================================================

  /**
   * Create a column reference.
   */
  def col(colName: String): Column =
    Column(Expression(
      exprType = Expression.ExprType.UnresolvedAttribute(
        Expression.UnresolvedAttribute(unparsedIdentifier = colName)
      )
    ))

  /**
   * Create a column reference (alias for col).
   */
  def column(colName: String): Column =
    col(colName)

  /**
   * Create a literal column.
   */
  def lit(value: Any): Column =
    val literal = value match
      case null =>
        Expression.Literal(
          literalType = Expression.Literal.LiteralType.Null(ProtoDataType())
        )
      case b: Boolean =>
        Expression.Literal(
          literalType = Expression.Literal.LiteralType.Boolean(b)
        )
      case b: Byte =>
        Expression.Literal(
          literalType = Expression.Literal.LiteralType.Byte(b.toInt)
        )
      case s: Short =>
        Expression.Literal(
          literalType = Expression.Literal.LiteralType.Short(s.toInt)
        )
      case i: Int =>
        Expression.Literal(
          literalType = Expression.Literal.LiteralType.Integer(i)
        )
      case l: Long =>
        Expression.Literal(
          literalType = Expression.Literal.LiteralType.Long(l)
        )
      case f: Float =>
        Expression.Literal(
          literalType = Expression.Literal.LiteralType.Float(f)
        )
      case d: Double =>
        Expression.Literal(
          literalType = Expression.Literal.LiteralType.Double(d)
        )
      case s: String =>
        Expression.Literal(
          literalType = Expression.Literal.LiteralType.String(s)
        )
      case _ =>
        Expression.Literal(
          literalType = Expression.Literal.LiteralType.String(value.toString)
        )

    Column(Expression(
      exprType = Expression.ExprType.Literal(literal)
    ))

  /**
   * Parse a SQL expression string into a Column.
   */
  def expr(expression: String): Column =
    Column(Expression(
      exprType = Expression.ExprType.ExpressionString(
        Expression.ExpressionString(expression = expression)
      )
    ))

  // ============================================================================
  // Aggregate functions
  // ============================================================================

  /**
   * Count function.
   */
  def count(col: Column): Column =
    callFunction("count", col)

  /**
   * Sum function.
   */
  def sum(col: Column): Column =
    callFunction("sum", col)

  /**
   * Average function.
   */
  def avg(col: Column): Column =
    callFunction("avg", col)

  /**
   * Mean function (alias for avg).
   */
  def mean(col: Column): Column =
    avg(col)

  /**
   * Maximum function.
   */
  def max(col: Column): Column =
    callFunction("max", col)

  /**
   * Minimum function.
   */
  def min(col: Column): Column =
    callFunction("min", col)

  /**
   * First value in a group.
   */
  def first(col: Column): Column =
    callFunction("first", col)

  /**
   * Last value in a group.
   */
  def last(col: Column): Column =
    callFunction("last", col)

  /**
   * Count distinct values.
   */
  def countDistinct(col: Column, cols: Column*): Column =
    callFunction("count_distinct", (col +: cols)*)

  // ============================================================================
  // String functions
  // ============================================================================

  /**
   * Concatenate strings.
   */
  def concat(cols: Column*): Column =
    callFunction("concat", cols*)

  /**
   * Convert string to uppercase.
   */
  def upper(col: Column): Column =
    callFunction("upper", col)

  /**
   * Convert string to lowercase.
   */
  def lower(col: Column): Column =
    callFunction("lower", col)

  /**
   * Trim spaces from both ends.
   */
  def trim(col: Column): Column =
    callFunction("trim", col)

  /**
   * Trim spaces from left end.
   */
  def ltrim(col: Column): Column =
    callFunction("ltrim", col)

  /**
   * Trim spaces from right end.
   */
  def rtrim(col: Column): Column =
    callFunction("rtrim", col)

  /**
   * Get substring.
   */
  def substring(col: Column, pos: Int, len: Int): Column =
    callFunction("substring", col, lit(pos), lit(len))

  /**
   * Get length of string.
   */
  def length(col: Column): Column =
    callFunction("length", col)

  /**
   * Replace occurrences of a string.
   */
  def replace(col: Column, search: String, replacement: String): Column =
    callFunction("replace", col, lit(search), lit(replacement))

  // ============================================================================
  // Math functions
  // ============================================================================

  /**
   * Absolute value.
   */
  def abs(col: Column): Column =
    callFunction("abs", col)

  /**
   * Square root.
   */
  def sqrt(col: Column): Column =
    callFunction("sqrt", col)

  /**
   * Power function.
   */
  def pow(col: Column, n: Double): Column =
    callFunction("pow", col, lit(n))

  /**
   * Round to n decimal places.
   */
  def round(col: Column, scale: Int = 0): Column =
    if scale == 0 then
      callFunction("round", col)
    else
      callFunction("round", col, lit(scale))

  /**
   * Floor function.
   */
  def floor(col: Column): Column =
    callFunction("floor", col)

  /**
   * Ceiling function.
   */
  def ceil(col: Column): Column =
    callFunction("ceil", col)

  /**
   * Natural logarithm.
   */
  def log(col: Column): Column =
    callFunction("ln", col)

  /**
   * Logarithm with base.
   */
  def log(base: Double, col: Column): Column =
    callFunction("log", lit(base), col)

  /**
   * Exponential function.
   */
  def exp(col: Column): Column =
    callFunction("exp", col)

  // ============================================================================
  // Date/Time functions
  // ============================================================================

  /**
   * Get current date.
   */
  def current_date(): Column =
    callFunction("current_date")

  /**
   * Get current timestamp.
   */
  def current_timestamp(): Column =
    callFunction("current_timestamp")

  /**
   * Extract year from date.
   */
  def year(col: Column): Column =
    callFunction("year", col)

  /**
   * Extract month from date.
   */
  def month(col: Column): Column =
    callFunction("month", col)

  /**
   * Extract day of month from date.
   */
  def dayofmonth(col: Column): Column =
    callFunction("dayofmonth", col)

  /**
   * Extract hour from timestamp.
   */
  def hour(col: Column): Column =
    callFunction("hour", col)

  /**
   * Extract minute from timestamp.
   */
  def minute(col: Column): Column =
    callFunction("minute", col)

  /**
   * Extract second from timestamp.
   */
  def second(col: Column): Column =
    callFunction("second", col)

  /**
   * Add days to a date.
   */
  def date_add(col: Column, days: Int): Column =
    callFunction("date_add", col, lit(days))

  /**
   * Subtract days from a date.
   */
  def date_sub(col: Column, days: Int): Column =
    callFunction("date_sub", col, lit(days))

  // ============================================================================
  // Null handling functions
  // ============================================================================

  /**
   * Coalesce - return first non-null value.
   */
  def coalesce(cols: Column*): Column =
    callFunction("coalesce", cols*)

  /**
   * Return null if two columns are equal.
   */
  def nullif(col1: Column, col2: Column): Column =
    callFunction("nullif", col1, col2)

  /**
   * Fill null values.
   */
  def nvl(col1: Column, col2: Column): Column =
    callFunction("nvl", col1, col2)

  // ============================================================================
  // Conditional functions
  // ============================================================================

  /**
   * Conditional when/otherwise expression.
   */
  def when(condition: Column, value: Any): Column =
    val valueCol = value match
      case c: Column => c
      case _ => lit(value)
    Column(Expression(
      exprType = Expression.ExprType.UnresolvedFunction(
        Expression.UnresolvedFunction(
          functionName = "when",
          arguments = Seq(condition.expr, valueCol.expr)
        )
      )
    ))

  // ============================================================================
  // Array functions
  // ============================================================================

  /**
   * Create an array column.
   */
  def array(cols: Column*): Column =
    callFunction("array", cols*)

  /**
   * Get size of array.
   */
  def size(col: Column): Column =
    callFunction("size", col)

  /**
   * Explode array into multiple rows.
   */
  def explode(col: Column): Column =
    callFunction("explode", col)

  // ============================================================================
  // Window functions
  // ============================================================================

  /**
   * Row number window function.
   */
  def row_number(): Column =
    callFunction("row_number")

  /**
   * Rank window function.
   */
  def rank(): Column =
    callFunction("rank")

  /**
   * Dense rank window function.
   */
  def dense_rank(): Column =
    callFunction("dense_rank")

  /**
   * Lead window function.
   */
  def lead(col: Column, offset: Int = 1): Column =
    callFunction("lead", col, lit(offset))

  /**
   * Lag window function.
   */
  def lag(col: Column, offset: Int = 1): Column =
    callFunction("lag", col, lit(offset))

  // ============================================================================
  // Helper function to call built-in functions
  // ============================================================================

  private def callFunction(name: String, args: Column*): Column =
    Column(Expression(
      exprType = Expression.ExprType.UnresolvedFunction(
        Expression.UnresolvedFunction(
          functionName = name,
          arguments = args.map(_.expr).toSeq
        )
      )
    ))
