package org.apache.spark.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.connect.proto.expressions.Expression

class FunctionsSpec extends AnyFlatSpec with Matchers:

  "functions.col" should "create a column reference" in {
    val col = functions.col("user_id")

    col.expr.exprType match
      case Expression.ExprType.UnresolvedAttribute(attr) =>
        attr.unparsedIdentifier shouldBe "user_id"
      case _ => fail("Expected UnresolvedAttribute")
  }

  "functions.lit" should "create literal for integer" in {
    val col = functions.lit(42)

    col.expr.exprType match
      case Expression.ExprType.Literal(literal) =>
        literal.literalType match
          case Expression.Literal.LiteralType.Integer(value) =>
            value shouldBe 42
          case _ => fail("Expected Integer literal")
      case _ => fail("Expected Literal")
  }

  "functions.lit" should "create literal for string" in {
    val col = functions.lit("hello")

    col.expr.exprType match
      case Expression.ExprType.Literal(literal) =>
        literal.literalType match
          case Expression.Literal.LiteralType.String(value) =>
            value shouldBe "hello"
          case _ => fail("Expected String literal")
      case _ => fail("Expected Literal")
  }

  "functions.lit" should "create literal for double" in {
    val col = functions.lit(3.14)

    col.expr.exprType match
      case Expression.ExprType.Literal(literal) =>
        literal.literalType match
          case Expression.Literal.LiteralType.Double(value) =>
            value shouldBe 3.14
          case _ => fail("Expected Double literal")
      case _ => fail("Expected Literal")
  }

  "functions.lit" should "create literal for boolean" in {
    val col = functions.lit(true)

    col.expr.exprType match
      case Expression.ExprType.Literal(literal) =>
        literal.literalType match
          case Expression.Literal.LiteralType.Boolean(value) =>
            value shouldBe true
          case _ => fail("Expected Boolean literal")
      case _ => fail("Expected Literal")
  }

  "functions.lit" should "create literal for long" in {
    val col = functions.lit(1000000L)

    col.expr.exprType match
      case Expression.ExprType.Literal(literal) =>
        literal.literalType match
          case Expression.Literal.LiteralType.Long(value) =>
            value shouldBe 1000000L
          case _ => fail("Expected Long literal")
      case _ => fail("Expected Literal")
  }

  "functions.lit" should "create null literal" in {
    val col = functions.lit(null)

    col.expr.exprType match
      case Expression.ExprType.Literal(literal) =>
        literal.literalType match
          case Expression.Literal.LiteralType.Null(_) => succeed
          case _ => fail("Expected Null literal")
      case _ => fail("Expected Literal")
  }

  "functions.expr" should "create SQL expression" in {
    val col = functions.expr("count(*)")

    col.expr.exprType match
      case Expression.ExprType.ExpressionString(exprString) =>
        exprString.expression shouldBe "count(*)"
      case _ => fail("Expected ExpressionString")
  }

  "aggregate functions" should "create correct function calls" in {
    val testCases = Seq(
      (functions.count(functions.col("id")), "count"),
      (functions.sum(functions.col("amount")), "sum"),
      (functions.avg(functions.col("score")), "avg"),
      (functions.min(functions.col("age")), "min"),
      (functions.max(functions.col("salary")), "max"),
      (functions.first(functions.col("name")), "first"),
      (functions.last(functions.col("value")), "last")
    )

    testCases.foreach { case (col, expectedFn) =>
      col.expr.exprType match
        case Expression.ExprType.UnresolvedFunction(fn) =>
          fn.functionName shouldBe expectedFn
        case _ => fail(s"Expected UnresolvedFunction for $expectedFn")
    }
  }

  "string functions" should "create correct function calls" in {
    val col = functions.col("text")

    val testCases = Seq(
      (functions.upper(col), "upper"),
      (functions.lower(col), "lower"),
      (functions.trim(col), "trim"),
      (functions.ltrim(col), "ltrim"),
      (functions.rtrim(col), "rtrim"),
      (functions.length(col), "length")
    )

    testCases.foreach { case (result, expectedFn) =>
      result.expr.exprType match
        case Expression.ExprType.UnresolvedFunction(fn) =>
          fn.functionName shouldBe expectedFn
        case _ => fail(s"Expected UnresolvedFunction for $expectedFn")
    }
  }

  "functions.concat" should "create concat function with multiple arguments" in {
    val result = functions.concat(
      functions.col("first_name"),
      functions.lit(" "),
      functions.col("last_name")
    )

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "concat"
        fn.arguments.length shouldBe 3
      case _ => fail("Expected UnresolvedFunction")
  }

  "functions.substring" should "create substring function" in {
    val result = functions.substring(functions.col("text"), 1, 10)

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "substring"
      case _ => fail("Expected UnresolvedFunction")
  }

  "math functions" should "create correct function calls" in {
    val col = functions.col("value")

    val testCases = Seq(
      (functions.abs(col), "abs"),
      (functions.sqrt(col), "sqrt"),
      (functions.ceil(col), "ceil"),
      (functions.floor(col), "floor"),
      (functions.round(col), "round"),
      (functions.exp(col), "exp"),
      (functions.log(col), "ln")  // log() is natural logarithm, uses "ln" function name
    )

    testCases.foreach { case (result, expectedFn) =>
      result.expr.exprType match
        case Expression.ExprType.UnresolvedFunction(fn) =>
          fn.functionName shouldBe expectedFn
        case _ => fail(s"Expected UnresolvedFunction for $expectedFn")
    }
  }

  "functions.pow" should "create power function" in {
    val result = functions.pow(functions.col("base"), 2)

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "pow"
        fn.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction")
  }

  "date/time functions" should "create correct function calls" in {
    val testCases = Seq(
      (functions.current_date(), "current_date"),
      (functions.current_timestamp(), "current_timestamp")
    )

    testCases.foreach { case (result, expectedFn) =>
      result.expr.exprType match
        case Expression.ExprType.UnresolvedFunction(fn) =>
          fn.functionName shouldBe expectedFn
        case _ => fail(s"Expected UnresolvedFunction for $expectedFn")
    }
  }

  "functions.year" should "extract year from date" in {
    val result = functions.year(functions.col("date"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "year"
      case _ => fail("Expected UnresolvedFunction")
  }

  "functions.month" should "extract month from date" in {
    val result = functions.month(functions.col("date"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "month"
      case _ => fail("Expected UnresolvedFunction")
  }

  "functions.dayofmonth" should "extract day from date" in {
    val result = functions.dayofmonth(functions.col("date"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "dayofmonth"
      case _ => fail("Expected UnresolvedFunction")
  }

  "functions.coalesce" should "create coalesce with multiple arguments" in {
    val result = functions.coalesce(
      functions.col("col1"),
      functions.col("col2"),
      functions.lit("default")
    )

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "coalesce"
        fn.arguments.length shouldBe 3
      case _ => fail("Expected UnresolvedFunction")
  }

  "functions.array" should "create array constructor" in {
    val result = functions.array(
      functions.lit(1),
      functions.lit(2),
      functions.lit(3)
    )

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "array"
        fn.arguments.length shouldBe 3
      case _ => fail("Expected UnresolvedFunction")
  }

  "functions.size" should "get array/map size" in {
    val result = functions.size(functions.col("array_col"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "size"
      case _ => fail("Expected UnresolvedFunction")
  }

  "functions.explode" should "create explode function" in {
    val result = functions.explode(functions.col("array_col"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "explode"
      case _ => fail("Expected UnresolvedFunction")
  }

  "functions.when" should "create when/otherwise expression" in {
    val result = functions.when(functions.col("age") < 18, functions.lit("minor"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(fn) =>
        fn.functionName shouldBe "when"
        fn.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction")
  }
