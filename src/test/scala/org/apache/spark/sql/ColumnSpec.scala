package org.apache.spark.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.connect.proto.expressions.Expression

class ColumnSpec extends AnyFlatSpec with Matchers:

  "Column.apply" should "create a column from name" in {
    val col = Column("id")

    col.expr.exprType match
      case Expression.ExprType.UnresolvedAttribute(attr) =>
        attr.unparsedIdentifier shouldBe "id"
      case _ => fail("Expected UnresolvedAttribute")
  }

  "Column.===" should "create equality expression" in {
    val col1 = Column("age")
    val col2 = Column("salary")

    val result = col1 === col2

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "=="
        fn.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.===" should "work with literals" in {
    val col = Column("status")
    val result = col === "active"

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "=="
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.=!=" should "create inequality expression" in {
    val col1 = Column("x")
    val col2 = Column("y")

    val result = col1 =!= col2

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "!="
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.<" should "create less than expression" in {
    val col = Column("age")
    val result = col < 18

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "<"
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.>" should "create greater than expression" in {
    val col = Column("salary")
    val result = col > 50000

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe ">"
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.<=" should "create less than or equal expression" in {
    val col = Column("score")
    val result = col <= 100

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "<="
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.>=" should "create greater than or equal expression" in {
    val col = Column("age")
    val result = col >= 21

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe ">="
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.&&" should "create logical AND expression" in {
    val col1 = Column("a")
    val col2 = Column("b")

    val result = col1 && col2

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "and"
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.||" should "create logical OR expression" in {
    val col1 = Column("x")
    val col2 = Column("y")

    val result = col1 || col2

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "or"
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.unary_!" should "create logical NOT expression" in {
    val col = Column("isActive")

    val result = !col

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "not"
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.+" should "create addition expression" in {
    val col1 = Column("a")
    val col2 = Column("b")

    val result = col1 + col2

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "+"
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.-" should "create subtraction expression" in {
    val col1 = Column("x")
    val col2 = Column("y")

    val result = col1 - col2

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "-"
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.*" should "create multiplication expression" in {
    val col = Column("price")

    val result = col * 2

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "*"
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column./" should "create division expression" in {
    val col = Column("total")

    val result = col / 10

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "/"
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.%" should "create modulo expression" in {
    val col = Column("id")

    val result = col % 5

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "%"
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.unary_-" should "create unary minus expression" in {
    val col = Column("value")

    val result = -col

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "-"
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.alias" should "create aliased expression" in {
    val col = Column("original_name")

    val result = col.alias("new_name")

    result.expr.exprType match
      case Expression.ExprType.Alias(alias) =>
        alias.name shouldBe Seq("new_name")
      case _ => fail("Expected Alias")
  }

  "Column.as" should "be alias for alias method" in {
    val col = Column("field")

    val result = col.as("renamed")

    result.expr.exprType match
      case Expression.ExprType.Alias(alias) =>
        alias.name shouldBe Seq("renamed")
      case _ => fail("Expected Alias")
  }

  "Column.cast" should "create cast expression" in {
    val col = Column("string_field")

    val result = col.cast("integer")

    result.expr.exprType match
      case Expression.ExprType.Cast(cast) =>
        cast.expr shouldBe defined
      case _ => fail("Expected Cast")
  }

  "Column.isNull" should "create isnull function" in {
    val col = Column("nullable_field")

    val result = col.isNull

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "isnull"
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.isNotNull" should "create isnotnull function" in {
    val col = Column("field")

    val result = col.isNotNull

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "isnotnull"
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.isNaN" should "create isnan function" in {
    val col = Column("double_field")

    val result = col.isNaN

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "isnan"
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.asc" should "create ascending sort order" in {
    val col = Column("name")

    val result = col.asc

    result.expr.exprType match
      case Expression.ExprType.SortOrder(sort) =>
        sort.direction shouldBe Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING
      case _ => fail("Expected SortOrder")
  }

  "Column.desc" should "create descending sort order" in {
    val col = Column("age")

    val result = col.desc

    result.expr.exprType match
      case Expression.ExprType.SortOrder(sort) =>
        sort.direction shouldBe Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING
      case _ => fail("Expected SortOrder")
  }

  "Column.contains" should "create contains function" in {
    val col = Column("text")

    val result = col.contains(Column("pattern"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "contains"
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.startsWith" should "create startswith function" in {
    val col = Column("name")

    val result = col.startsWith(Column("prefix"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "startswith"
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.endsWith" should "create endswith function" in {
    val col = Column("filename")

    val result = col.endsWith(Column("extension"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "endswith"
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.like" should "create like expression" in {
    val col = Column("name")

    val result = col.like("%john%")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "like"
      case _ => fail("Expected UnresolvedFunction")
  }

  "Column.rlike" should "create rlike expression" in {
    val col = Column("email")

    val result = col.rlike(".*@example\\.com")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "rlike"
      case _ => fail("Expected UnresolvedFunction")
  }
