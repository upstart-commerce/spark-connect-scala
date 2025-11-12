package org.apache.spark.sql

import org.apache.spark.connect.proto.expressions.Expression
import com.google.protobuf.ByteString

/**
 * A column in a DataFrame.
 *
 * Column represents an expression that can be used in DataFrame operations.
 * Columns are created from column names, literals, or expressions, and can
 * be combined using various operators to form complex expressions.
 *
 * Example:
 * {{{
 *   df.filter(col("age") > 18 && col("country") === "US")
 *   df.select(col("name"), col("salary") * 1.1 as "new_salary")
 * }}}
 */
final class Column private[sql] (private[sql] val expr: Expression):

  // ============================================================================
  // Comparison operators
  // ============================================================================

  /**
   * Equality test (===).
   */
  def ===(other: Column): Column =
    binaryOp("==", other)

  /**
   * Equality test (===).
   */
  def ===(literal: Any): Column =
    ===(functions.lit(literal))

  /**
   * Inequality test (!==).
   */
  def =!=(other: Column): Column =
    binaryOp("!=", other)

  /**
   * Inequality test (!==).
   */
  def =!=(literal: Any): Column =
    =!=(functions.lit(literal))

  /**
   * Less than (<).
   */
  def <(other: Column): Column =
    binaryOp("<", other)

  /**
   * Less than (<).
   */
  def <(literal: Any): Column =
    <(functions.lit(literal))

  /**
   * Less than or equal (<=).
   */
  def <=(other: Column): Column =
    binaryOp("<=", other)

  /**
   * Less than or equal (<=).
   */
  def <=(literal: Any): Column =
    <=(functions.lit(literal))

  /**
   * Greater than (>).
   */
  def >(other: Column): Column =
    binaryOp(">", other)

  /**
   * Greater than (>).
   */
  def >(literal: Any): Column =
    >(functions.lit(literal))

  /**
   * Greater than or equal (>=).
   */
  def >=(other: Column): Column =
    binaryOp(">=", other)

  /**
   * Greater than or equal (>=).
   */
  def >=(literal: Any): Column =
    >=(functions.lit(literal))

  // ============================================================================
  // Logical operators
  // ============================================================================

  /**
   * Logical AND (&&).
   */
  def &&(other: Column): Column =
    binaryOp("and", other)

  /**
   * Logical OR (||).
   */
  def ||(other: Column): Column =
    binaryOp("or", other)

  /**
   * Logical NOT (!).
   */
  def unary_! : Column =
    unaryOp("not")

  // ============================================================================
  // Arithmetic operators
  // ============================================================================

  /**
   * Addition (+).
   */
  def +(other: Column): Column =
    binaryOp("+", other)

  /**
   * Addition (+) with literal.
   */
  def plus(literal: Any): Column =
    this.+(functions.lit(literal))

  /**
   * Subtraction (-).
   */
  def -(other: Column): Column =
    binaryOp("-", other)

  /**
   * Subtraction (-).
   */
  def -(literal: Any): Column =
    -(functions.lit(literal))

  /**
   * Multiplication (*).
   */
  def *(other: Column): Column =
    binaryOp("*", other)

  /**
   * Multiplication (*).
   */
  def *(literal: Any): Column =
    *(functions.lit(literal))

  /**
   * Division (/).
   */
  def /(other: Column): Column =
    binaryOp("/", other)

  /**
   * Division (/).
   */
  def /(literal: Any): Column =
    /(functions.lit(literal))

  /**
   * Modulo (%).
   */
  def %(other: Column): Column =
    binaryOp("%", other)

  /**
   * Modulo (%).
   */
  def %(literal: Any): Column =
    %(functions.lit(literal))

  /**
   * Unary minus (-).
   */
  def unary_- : Column =
    unaryOp("-")

  // ============================================================================
  // Null handling
  // ============================================================================

  /**
   * Check if the column is null.
   */
  def isNull: Column =
    Column(Expression(
      exprType = Expression.ExprType.UnresolvedFunction(
        Expression.UnresolvedFunction(
          functionName = "isnull",
          arguments = Seq(expr)
        )
      )
    ))

  /**
   * Check if the column is not null.
   */
  def isNotNull: Column =
    Column(Expression(
      exprType = Expression.ExprType.UnresolvedFunction(
        Expression.UnresolvedFunction(
          functionName = "isnotnull",
          arguments = Seq(expr)
        )
      )
    ))

  /**
   * Check if the column is NaN.
   */
  def isNaN: Column =
    Column(Expression(
      exprType = Expression.ExprType.UnresolvedFunction(
        Expression.UnresolvedFunction(
          functionName = "isnan",
          arguments = Seq(expr)
        )
      )
    ))

  // ============================================================================
  // String operations
  // ============================================================================

  /**
   * String contains test.
   */
  def contains(other: Column): Column =
    Column(Expression(
      exprType = Expression.ExprType.UnresolvedFunction(
        Expression.UnresolvedFunction(
          functionName = "contains",
          arguments = Seq(expr, other.expr)
        )
      )
    ))

  /**
   * String startsWith test.
   */
  def startsWith(other: Column): Column =
    Column(Expression(
      exprType = Expression.ExprType.UnresolvedFunction(
        Expression.UnresolvedFunction(
          functionName = "startswith",
          arguments = Seq(expr, other.expr)
        )
      )
    ))

  /**
   * String endsWith test.
   */
  def endsWith(other: Column): Column =
    Column(Expression(
      exprType = Expression.ExprType.UnresolvedFunction(
        Expression.UnresolvedFunction(
          functionName = "endswith",
          arguments = Seq(expr, other.expr)
        )
      )
    ))

  /**
   * SQL LIKE expression.
   */
  def like(pattern: String): Column =
    binaryOp("like", functions.lit(pattern))

  /**
   * SQL RLIKE (regex like) expression.
   */
  def rlike(pattern: String): Column =
    binaryOp("rlike", functions.lit(pattern))

  // ============================================================================
  // Sorting
  // ============================================================================

  /**
   * Return a sort expression based on ascending order.
   */
  def asc: Column =
    Column(Expression(
      exprType = Expression.ExprType.SortOrder(
        Expression.SortOrder(
          child = Some(expr),
          direction = Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING,
          nullOrdering = Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST
        )
      )
    ))

  /**
   * Return a sort expression based on descending order.
   */
  def desc: Column =
    Column(Expression(
      exprType = Expression.ExprType.SortOrder(
        Expression.SortOrder(
          child = Some(expr),
          direction = Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING,
          nullOrdering = Expression.SortOrder.NullOrdering.SORT_NULLS_LAST
        )
      )
    ))

  /**
   * Convert sort order to protobuf SortOrder.
   */
  private[sql] def toSortOrder: Expression.SortOrder =
    expr.exprType match
      case Expression.ExprType.SortOrder(sortOrder) =>
        sortOrder
      case Expression.ExprType.Empty =>
        // Default to ascending if not already a sort order
        Expression.SortOrder(
          child = Some(expr),
          direction = Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING,
          nullOrdering = Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST
        )
      case _ =>
        // Default to ascending for all other expression types
        Expression.SortOrder(
          child = Some(expr),
          direction = Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING,
          nullOrdering = Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST
        )

  // ============================================================================
  // Aliasing and casting
  // ============================================================================

  /**
   * Give the column an alias.
   */
  def alias(name: String): Column =
    Column(Expression(
      exprType = Expression.ExprType.Alias(
        Expression.Alias(
          expr = Some(expr),
          name = Seq(name)
        )
      )
    ))

  /**
   * Give the column an alias (same as alias).
   */
  def as(name: String): Column =
    alias(name)

  /**
   * Cast the column to a different data type.
   */
  def cast(dataType: String): Column =
    Column(Expression(
      exprType = Expression.ExprType.Cast(
        Expression.Cast(
          expr = Some(expr),
          castToType = Expression.Cast.CastToType.Type(dataTypeFromString(dataType))
        )
      )
    ))

  // ============================================================================
  // Collection operations
  // ============================================================================

  /**
   * Get an item from an array or map column.
   */
  def apply(key: Any): Column =
    getItem(key)

  /**
   * Get an item from an array or map column.
   */
  def getItem(key: Any): Column =
    Column(Expression(
      exprType = Expression.ExprType.UnresolvedFunction(
        Expression.UnresolvedFunction(
          functionName = "getitem",
          arguments = Seq(expr, functions.lit(key).expr)
        )
      )
    ))

  /**
   * Get a field from a struct column.
   */
  def getField(fieldName: String): Column =
    Column(Expression(
      exprType = Expression.ExprType.UnresolvedFunction(
        Expression.UnresolvedFunction(
          functionName = "getfield",
          arguments = Seq(expr, functions.lit(fieldName).expr)
        )
      )
    ))

  // ============================================================================
  // Helper methods
  // ============================================================================

  private def binaryOp(op: String, other: Column): Column =
    Column(Expression(
      exprType = Expression.ExprType.UnresolvedFunction(
        Expression.UnresolvedFunction(
          functionName = op,
          arguments = Seq(expr, other.expr)
        )
      )
    ))

  private def unaryOp(op: String): Column =
    Column(Expression(
      exprType = Expression.ExprType.UnresolvedFunction(
        Expression.UnresolvedFunction(
          functionName = op,
          arguments = Seq(expr)
        )
      )
    ))

  private def dataTypeFromString(dataType: String): org.apache.spark.connect.proto.types.DataType =
    // Simplified type conversion
    import org.apache.spark.connect.proto.types.DataType
    dataType.toLowerCase match
      case "string" => DataType(kind = DataType.Kind.String(DataType.String()))
      case "int" | "integer" => DataType(kind = DataType.Kind.Integer(DataType.Integer()))
      case "long" | "bigint" => DataType(kind = DataType.Kind.Long(DataType.Long()))
      case "double" => DataType(kind = DataType.Kind.Double(DataType.Double()))
      case "float" => DataType(kind = DataType.Kind.Float(DataType.Float()))
      case "boolean" => DataType(kind = DataType.Kind.Boolean(DataType.Boolean()))
      case "date" => DataType(kind = DataType.Kind.Date(DataType.Date()))
      case "timestamp" => DataType(kind = DataType.Kind.Timestamp(DataType.Timestamp()))
      case _ => DataType(kind = DataType.Kind.String(DataType.String())) // Default to string

  override def toString: String = expr.toString

object Column:

  /**
   * Create a column from a column name.
   */
  def apply(name: String): Column =
    Column(Expression(
      exprType = Expression.ExprType.UnresolvedAttribute(
        Expression.UnresolvedAttribute(unparsedIdentifier = name)
      )
    ))

  /**
   * Create a column from an expression.
   */
  private[sql] def apply(expr: Expression): Column =
    new Column(expr)
