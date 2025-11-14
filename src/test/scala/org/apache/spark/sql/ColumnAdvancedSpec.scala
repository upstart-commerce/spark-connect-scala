package org.apache.spark.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.connect.proto.expressions.Expression

class ColumnAdvancedSpec extends AnyFlatSpec with Matchers:

  // ===========================================================================
  // Predicate Tests
  // ===========================================================================

  "Column.isin" should "create IN expression" in {
    val col = functions.col("status")
    val result = col.isin("active", "pending", "completed")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "in"
        func.arguments.length shouldBe 4 // column + 3 values
      case _ => fail("Expected UnresolvedFunction with 'in'")
  }

  "Column.isin with single value" should "create IN expression" in {
    val col = functions.col("id")
    val result = col.isin(42)

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "in"
        func.arguments.length shouldBe 2 // column + 1 value
      case _ => fail("Expected UnresolvedFunction with 'in'")
  }

  "Column.isin with empty values" should "create IN expression" in {
    val col = functions.col("name")
    val result = col.isin()

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "in"
        func.arguments.length shouldBe 1 // just column
      case _ => fail("Expected UnresolvedFunction with 'in'")
  }

  "Column.between" should "create range condition" in {
    val col = functions.col("age")
    val result = col.between(18, 65)

    // between is implemented as (col >= lower) && (col <= upper)
    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "and"
      case _ => fail("Expected AND expression")
  }

  "Column.between with strings" should "create range condition" in {
    val col = functions.col("name")
    val result = col.between("A", "M")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "and"
      case _ => fail("Expected AND expression")
  }

  // ===========================================================================
  // String Operations Tests
  // ===========================================================================

  "Column.substr" should "create substring expression" in {
    val col = functions.col("text")
    val result = col.substr(1, 10)

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "substring"
        func.arguments.length shouldBe 3 // column, start, length
      case _ => fail("Expected UnresolvedFunction with 'substring'")
  }

  "Column.substring" should "be alias for substr" in {
    val col = functions.col("text")
    val result = col.substring(5, 20)

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "substring"
      case _ => fail("Expected UnresolvedFunction with 'substring'")
  }

  "Column.contains" should "create contains expression" in {
    val col = functions.col("description")
    val result = col.contains(functions.lit("spark"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "contains"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'contains'")
  }

  "Column.startsWith" should "create startsWith expression" in {
    val col = functions.col("name")
    val result = col.startsWith(functions.lit("Mr."))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "startswith"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'startswith'")
  }

  "Column.endsWith" should "create endsWith expression" in {
    val col = functions.col("filename")
    val result = col.endsWith(functions.lit(".txt"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "endswith"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'endswith'")
  }

  "Column.like" should "create LIKE expression" in {
    val col = functions.col("email")
    val result = col.like("%@example.com")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "like"
      case _ => fail("Expected UnresolvedFunction with 'like'")
  }

  "Column.rlike" should "create RLIKE expression" in {
    val col = functions.col("phone")
    val result = col.rlike("\\d{3}-\\d{3}-\\d{4}")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "rlike"
      case _ => fail("Expected UnresolvedFunction with 'rlike'")
  }

  // ===========================================================================
  // Null Handling Tests
  // ===========================================================================

  "Column.isNull" should "create isNull expression" in {
    val col = functions.col("optional_field")
    val result = col.isNull

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "isnull"
        func.arguments.length shouldBe 1
      case _ => fail("Expected UnresolvedFunction with 'isnull'")
  }

  "Column.isNotNull" should "create isNotNull expression" in {
    val col = functions.col("required_field")
    val result = col.isNotNull

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "isnotnull"
        func.arguments.length shouldBe 1
      case _ => fail("Expected UnresolvedFunction with 'isnotnull'")
  }

  "Column.isNaN" should "create isNaN expression" in {
    val col = functions.col("float_value")
    val result = col.isNaN

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "isnan"
        func.arguments.length shouldBe 1
      case _ => fail("Expected UnresolvedFunction with 'isnan'")
  }

  // ===========================================================================
  // Collection Operations Tests
  // ===========================================================================

  "Column.apply" should "create getItem expression for arrays" in {
    val col = functions.col("items")
    val result = col(0)

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "getitem"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'getitem'")
  }

  "Column.getItem" should "create getItem expression" in {
    val col = functions.col("data")
    val result = col.getItem("key")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "getitem"
      case _ => fail("Expected UnresolvedFunction with 'getitem'")
  }

  "Column.getField" should "create getField expression for structs" in {
    val col = functions.col("address")
    val result = col.getField("street")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "getfield"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'getfield'")
  }

  // ===========================================================================
  // Sorting Tests
  // ===========================================================================

  "Column.asc" should "create ascending sort order" in {
    val col = functions.col("price")
    val result = col.asc

    result.expr.exprType match
      case Expression.ExprType.SortOrder(sortOrder) =>
        sortOrder.direction shouldBe Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING
        sortOrder.nullOrdering shouldBe Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST
      case _ => fail("Expected SortOrder expression")
  }

  "Column.desc" should "create descending sort order" in {
    val col = functions.col("timestamp")
    val result = col.desc

    result.expr.exprType match
      case Expression.ExprType.SortOrder(sortOrder) =>
        sortOrder.direction shouldBe Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING
        sortOrder.nullOrdering shouldBe Expression.SortOrder.NullOrdering.SORT_NULLS_LAST
      case _ => fail("Expected SortOrder expression")
  }

  // ===========================================================================
  // Aliasing and Casting Tests
  // ===========================================================================

  "Column.alias" should "create Alias expression" in {
    val col = functions.col("original_name")
    val result = col.alias("new_name")

    result.expr.exprType match
      case Expression.ExprType.Alias(alias) =>
        alias.name shouldBe Seq("new_name")
        alias.expr shouldBe defined
      case _ => fail("Expected Alias expression")
  }

  "Column.as" should "be alias for alias" in {
    val col = functions.col("old")
    val result = col.as("new")

    result.expr.exprType match
      case Expression.ExprType.Alias(alias) =>
        alias.name shouldBe Seq("new")
      case _ => fail("Expected Alias expression")
  }

  "Column.cast to string" should "create Cast expression" in {
    val col = functions.col("id")
    val result = col.cast("string")

    result.expr.exprType match
      case Expression.ExprType.Cast(cast) =>
        cast.expr shouldBe defined
      case _ => fail("Expected Cast expression")
  }

  "Column.cast to int" should "create Cast expression" in {
    val col = functions.col("string_number")
    val result = col.cast("int")

    result.expr.exprType match
      case Expression.ExprType.Cast(cast) =>
        cast.expr shouldBe defined
      case _ => fail("Expected Cast expression")
  }

  "Column.cast to double" should "create Cast expression" in {
    val col = functions.col("value")
    val result = col.cast("double")

    result.expr.exprType match
      case Expression.ExprType.Cast(cast) =>
        cast.expr shouldBe defined
      case _ => fail("Expected Cast expression")
  }

  // ===========================================================================
  // Arithmetic Operators Tests
  // ===========================================================================

  "Column.plus" should "create addition expression" in {
    val col = functions.col("price")
    val result = col.plus(10)

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "+"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with '+'")
  }

  "Column unary minus" should "create negation expression" in {
    val col = functions.col("value")
    val result = -col

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "-"
        func.arguments.length shouldBe 1
      case _ => fail("Expected UnresolvedFunction with '-'")
  }

  "Column unary not" should "create logical NOT expression" in {
    val col = functions.col("is_active")
    val result = !col

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "not"
        func.arguments.length shouldBe 1
      case _ => fail("Expected UnresolvedFunction with 'not'")
  }
