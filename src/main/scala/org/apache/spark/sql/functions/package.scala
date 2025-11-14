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

  /**
   * Collect list of values (aggregate).
   */
  def collect_list(col: Column): Column =
    callFunction("collect_list", col)

  /**
   * Collect set of unique values (aggregate).
   */
  def collect_set(col: Column): Column =
    callFunction("collect_set", col)

  /**
   * Standard deviation (sample).
   */
  def stddev(col: Column): Column =
    callFunction("stddev", col)

  /**
   * Standard deviation (population).
   */
  def stddev_pop(col: Column): Column =
    callFunction("stddev_pop", col)

  /**
   * Standard deviation (sample) - alias.
   */
  def stddev_samp(col: Column): Column =
    callFunction("stddev_samp", col)

  /**
   * Variance (sample).
   */
  def variance(col: Column): Column =
    callFunction("variance", col)

  /**
   * Variance (population).
   */
  def var_pop(col: Column): Column =
    callFunction("var_pop", col)

  /**
   * Variance (sample) - alias.
   */
  def var_samp(col: Column): Column =
    callFunction("var_samp", col)

  /**
   * Correlation between two columns.
   */
  def corr(col1: Column, col2: Column): Column =
    callFunction("corr", col1, col2)

  /**
   * Population covariance.
   */
  def covar_pop(col1: Column, col2: Column): Column =
    callFunction("covar_pop", col1, col2)

  /**
   * Sample covariance.
   */
  def covar_samp(col1: Column, col2: Column): Column =
    callFunction("covar_samp", col1, col2)

  /**
   * Skewness.
   */
  def skewness(col: Column): Column =
    callFunction("skewness", col)

  /**
   * Kurtosis.
   */
  def kurtosis(col: Column): Column =
    callFunction("kurtosis", col)

  /**
   * Approximate percentile.
   */
  def percentile_approx(col: Column, percentage: Column, accuracy: Column): Column =
    callFunction("percentile_approx", col, percentage, accuracy)

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

  /**
   * Ntile window function.
   */
  def ntile(n: Int): Column =
    callFunction("ntile", lit(n))

  // ============================================================================
  // More Date/Time functions
  // ============================================================================

  /**
   * Convert string to date.
   */
  def to_date(col: Column): Column =
    callFunction("to_date", col)

  /**
   * Convert string to date with format.
   */
  def to_date(col: Column, format: String): Column =
    callFunction("to_date", col, lit(format))

  /**
   * Convert string to timestamp.
   */
  def to_timestamp(col: Column): Column =
    callFunction("to_timestamp", col)

  /**
   * Convert string to timestamp with format.
   */
  def to_timestamp(col: Column, format: String): Column =
    callFunction("to_timestamp", col, lit(format))

  /**
   * Format date as string.
   */
  def date_format(col: Column, format: String): Column =
    callFunction("date_format", col, lit(format))

  /**
   * Extract week of year.
   */
  def weekofyear(col: Column): Column =
    callFunction("weekofyear", col)

  /**
   * Extract day of week.
   */
  def dayofweek(col: Column): Column =
    callFunction("dayofweek", col)

  /**
   * Extract day of year.
   */
  def dayofyear(col: Column): Column =
    callFunction("dayofyear", col)

  /**
   * Extract quarter from date.
   */
  def quarter(col: Column): Column =
    callFunction("quarter", col)

  /**
   * Unix timestamp (seconds since epoch).
   */
  def unix_timestamp(): Column =
    callFunction("unix_timestamp")

  /**
   * Unix timestamp from string with format.
   */
  def unix_timestamp(col: Column, format: String): Column =
    callFunction("unix_timestamp", col, lit(format))

  /**
   * Convert unix timestamp to timestamp.
   */
  def from_unixtime(col: Column): Column =
    callFunction("from_unixtime", col)

  /**
   * Convert unix timestamp to timestamp with format.
   */
  def from_unixtime(col: Column, format: String): Column =
    callFunction("from_unixtime", col, lit(format))

  /**
   * Add months to a date.
   */
  def add_months(col: Column, months: Int): Column =
    callFunction("add_months", col, lit(months))

  /**
   * Months between two dates.
   */
  def months_between(date1: Column, date2: Column): Column =
    callFunction("months_between", date1, date2)

  /**
   * Last day of month.
   */
  def last_day(col: Column): Column =
    callFunction("last_day", col)

  /**
   * Next day after given date.
   */
  def next_day(col: Column, dayOfWeek: String): Column =
    callFunction("next_day", col, lit(dayOfWeek))

  /**
   * Truncate date to specified unit.
   */
  def trunc(col: Column, format: String): Column =
    callFunction("trunc", col, lit(format))

  /**
   * Truncate timestamp to specified unit.
   */
  def date_trunc(format: String, col: Column): Column =
    callFunction("date_trunc", lit(format), col)

  // ============================================================================
  // More String functions
  // ============================================================================

  /**
   * Concatenate strings with separator.
   */
  def concat_ws(sep: String, cols: Column*): Column =
    callFunction("concat_ws", lit(sep) +: cols*)

  /**
   * Format string.
   */
  def format_string(format: String, cols: Column*): Column =
    callFunction("format_string", lit(format) +: cols*)

  /**
   * Locate substring position.
   */
  def locate(substr: String, col: Column): Column =
    callFunction("locate", lit(substr), col)

  /**
   * Locate substring position with start position.
   */
  def locate(substr: String, col: Column, pos: Int): Column =
    callFunction("locate", lit(substr), col, lit(pos))

  /**
   * Left pad string.
   */
  def lpad(col: Column, len: Int, pad: String): Column =
    callFunction("lpad", col, lit(len), lit(pad))

  /**
   * Right pad string.
   */
  def rpad(col: Column, len: Int, pad: String): Column =
    callFunction("rpad", col, lit(len), lit(pad))

  /**
   * Translate characters.
   */
  def translate(col: Column, matching: String, replace: String): Column =
    callFunction("translate", col, lit(matching), lit(replace))

  // ============================================================================
  // More Array functions
  // ============================================================================

  /**
   * Check if array contains value.
   */
  def array_contains(col: Column, value: Any): Column =
    callFunction("array_contains", col, lit(value))

  /**
   * Get distinct values from array.
   */
  def array_distinct(col: Column): Column =
    callFunction("array_distinct", col)

  /**
   * Union of arrays.
   */
  def array_union(col1: Column, col2: Column): Column =
    callFunction("array_union", col1, col2)

  /**
   * Intersection of arrays.
   */
  def array_intersect(col1: Column, col2: Column): Column =
    callFunction("array_intersect", col1, col2)

  /**
   * Difference of arrays.
   */
  def array_except(col1: Column, col2: Column): Column =
    callFunction("array_except", col1, col2)

  /**
   * Join array elements with delimiter.
   */
  def array_join(col: Column, delimiter: String): Column =
    callFunction("array_join", col, lit(delimiter))

  /**
   * Maximum value in array.
   */
  def array_max(col: Column): Column =
    callFunction("array_max", col)

  /**
   * Minimum value in array.
   */
  def array_min(col: Column): Column =
    callFunction("array_min", col)

  /**
   * Position of element in array (1-indexed).
   */
  def array_position(col: Column, value: Any): Column =
    callFunction("array_position", col, lit(value))

  /**
   * Remove element from array.
   */
  def array_remove(col: Column, element: Any): Column =
    callFunction("array_remove", col, lit(element))

  /**
   * Repeat array n times.
   */
  def array_repeat(col: Column, count: Int): Column =
    callFunction("array_repeat", col, lit(count))

  /**
   * Sort array.
   */
  def array_sort(col: Column): Column =
    callFunction("array_sort", col)

  /**
   * Flatten nested arrays.
   */
  def flatten(col: Column): Column =
    callFunction("flatten", col)

  /**
   * Reverse array.
   */
  def reverse(col: Column): Column =
    callFunction("reverse", col)

  /**
   * Shuffle array elements.
   */
  def shuffle(col: Column): Column =
    callFunction("shuffle", col)

  /**
   * Slice array.
   */
  def slice(col: Column, start: Int, length: Int): Column =
    callFunction("slice", col, lit(start), lit(length))

  /**
   * Sort array with custom order.
   */
  def sort_array(col: Column, asc: Boolean = true): Column =
    callFunction("sort_array", col, lit(asc))

  /**
   * Zip arrays together.
   */
  def arrays_zip(cols: Column*): Column =
    callFunction("arrays_zip", cols*)

  /**
   * Check if arrays overlap.
   */
  def arrays_overlap(col1: Column, col2: Column): Column =
    callFunction("arrays_overlap", col1, col2)

  // ============================================================================
  // Map functions
  // ============================================================================

  /**
   * Create a map from arrays of keys and values.
   */
  def map_from_arrays(keys: Column, values: Column): Column =
    callFunction("map_from_arrays", keys, values)

  /**
   * Get map keys.
   */
  def map_keys(col: Column): Column =
    callFunction("map_keys", col)

  /**
   * Get map values.
   */
  def map_values(col: Column): Column =
    callFunction("map_values", col)

  /**
   * Check if map contains key.
   */
  def map_contains_key(col: Column, key: Any): Column =
    callFunction("map_contains_key", col, lit(key))

  // ============================================================================
  // Struct functions
  // ============================================================================

  /**
   * Create a struct column.
   */
  def struct(cols: Column*): Column =
    callFunction("struct", cols*)

  /**
   * Extract JSON object field.
   */
  def get_json_object(col: Column, path: String): Column =
    callFunction("get_json_object", col, lit(path))

  /**
   * Convert struct to JSON.
   */
  def to_json(col: Column): Column =
    callFunction("to_json", col)

  /**
   * Parse JSON string to struct.
   */
  def from_json(col: Column, schema: Column): Column =
    callFunction("from_json", col, schema)

  // ============================================================================
  // More Math functions
  // ============================================================================

  /**
   * Cosine.
   */
  def cos(col: Column): Column =
    callFunction("cos", col)

  /**
   * Tangent.
   */
  def tan(col: Column): Column =
    callFunction("tan", col)

  /**
   * Arc sine.
   */
  def asin(col: Column): Column =
    callFunction("asin", col)

  /**
   * Arc cosine.
   */
  def acos(col: Column): Column =
    callFunction("acos", col)

  /**
   * Arc tangent.
   */
  def atan(col: Column): Column =
    callFunction("atan", col)

  /**
   * Arc tangent of y/x.
   */
  def atan2(y: Column, x: Column): Column =
    callFunction("atan2", y, x)

  /**
   * Hyperbolic sine.
   */
  def sinh(col: Column): Column =
    callFunction("sinh", col)

  /**
   * Hyperbolic cosine.
   */
  def cosh(col: Column): Column =
    callFunction("cosh", col)

  /**
   * Hyperbolic tangent.
   */
  def tanh(col: Column): Column =
    callFunction("tanh", col)

  /**
   * Convert radians to degrees.
   */
  def degrees(col: Column): Column =
    callFunction("degrees", col)

  /**
   * Convert degrees to radians.
   */
  def radians(col: Column): Column =
    callFunction("radians", col)

  /**
   * Greatest of values.
   */
  def greatest(cols: Column*): Column =
    callFunction("greatest", cols*)

  /**
   * Least of values.
   */
  def least(cols: Column*): Column =
    callFunction("least", cols*)

  /**
   * Hypot (hypotenuse).
   */
  def hypot(l: Column, r: Column): Column =
    callFunction("hypot", l, r)

  /**
   * Cube root.
   */
  def cbrt(col: Column): Column =
    callFunction("cbrt", col)

  /**
   * Factorial.
   */
  def factorial(col: Column): Column =
    callFunction("factorial", col)

  /**
   * Modulo that matches Python's % operator.
   */
  def pmod(dividend: Column, divisor: Column): Column =
    callFunction("pmod", dividend, divisor)

  /**
   * Random value between 0 and 1.
   */
  def rand(seed: Long): Column =
    callFunction("rand", lit(seed))

  /**
   * Random value from standard normal distribution.
   */
  def randn(seed: Long): Column =
    callFunction("randn", lit(seed))

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
