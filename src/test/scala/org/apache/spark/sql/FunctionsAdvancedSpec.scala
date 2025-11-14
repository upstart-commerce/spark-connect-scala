package org.apache.spark.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.connect.proto.expressions.Expression

class FunctionsAdvancedSpec extends AnyFlatSpec with Matchers:

  // ===========================================================================
  // Aggregate Functions Tests
  // ===========================================================================

  "functions.collect_list" should "create collect_list expression" in {
    val result = functions.collect_list(functions.col("values"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "collect_list"
        func.arguments.length shouldBe 1
      case _ => fail("Expected UnresolvedFunction with 'collect_list'")
  }

  "functions.collect_set" should "create collect_set expression" in {
    val result = functions.collect_set(functions.col("values"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "collect_set"
      case _ => fail("Expected UnresolvedFunction with 'collect_set'")
  }

  "functions.stddev" should "create stddev expression" in {
    val result = functions.stddev(functions.col("measurements"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "stddev"
      case _ => fail("Expected UnresolvedFunction with 'stddev'")
  }

  "functions.variance" should "create variance expression" in {
    val result = functions.variance(functions.col("data"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "variance"
      case _ => fail("Expected UnresolvedFunction with 'variance'")
  }

  "functions.corr" should "create correlation expression" in {
    val result = functions.corr(functions.col("x"), functions.col("y"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "corr"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'corr'")
  }

  "functions.covar_pop" should "create population covariance expression" in {
    val result = functions.covar_pop(functions.col("a"), functions.col("b"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "covar_pop"
      case _ => fail("Expected UnresolvedFunction with 'covar_pop'")
  }

  // ===========================================================================
  // Date/Time Functions Tests
  // ===========================================================================

  "functions.to_date" should "create to_date expression without format" in {
    val result = functions.to_date(functions.col("date_string"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "to_date"
        func.arguments.length shouldBe 1
      case _ => fail("Expected UnresolvedFunction with 'to_date'")
  }

  "functions.to_date with format" should "create to_date expression with format" in {
    val result = functions.to_date(functions.col("date_string"), "yyyy-MM-dd")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "to_date"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'to_date'")
  }

  "functions.to_timestamp" should "create to_timestamp expression" in {
    val result = functions.to_timestamp(functions.col("timestamp_string"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "to_timestamp"
      case _ => fail("Expected UnresolvedFunction with 'to_timestamp'")
  }

  "functions.date_format" should "create date_format expression" in {
    val result = functions.date_format(functions.col("date"), "yyyy-MM-dd")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "date_format"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'date_format'")
  }

  "functions.year" should "create year extraction expression" in {
    val result = functions.year(functions.col("date"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "year"
      case _ => fail("Expected UnresolvedFunction with 'year'")
  }

  "functions.weekofyear" should "create week of year extraction" in {
    val result = functions.weekofyear(functions.col("date"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "weekofyear"
      case _ => fail("Expected UnresolvedFunction with 'weekofyear'")
  }

  "functions.dayofweek" should "create day of week extraction" in {
    val result = functions.dayofweek(functions.col("date"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "dayofweek"
      case _ => fail("Expected UnresolvedFunction with 'dayofweek'")
  }

  "functions.unix_timestamp" should "create current unix timestamp" in {
    val result = functions.unix_timestamp()

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "unix_timestamp"
      case _ => fail("Expected UnresolvedFunction with 'unix_timestamp'")
  }

  "functions.from_unixtime" should "create from_unixtime expression" in {
    val result = functions.from_unixtime(functions.col("timestamp"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "from_unixtime"
      case _ => fail("Expected UnresolvedFunction with 'from_unixtime'")
  }

  "functions.add_months" should "create add_months expression" in {
    val result = functions.add_months(functions.col("date"), 3)

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "add_months"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'add_months'")
  }

  "functions.months_between" should "create months_between expression" in {
    val result = functions.months_between(functions.col("end_date"), functions.col("start_date"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "months_between"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'months_between'")
  }

  "functions.last_day" should "create last_day expression" in {
    val result = functions.last_day(functions.col("date"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "last_day"
      case _ => fail("Expected UnresolvedFunction with 'last_day'")
  }

  "functions.next_day" should "create next_day expression" in {
    val result = functions.next_day(functions.col("date"), "Monday")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "next_day"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'next_day'")
  }

  "functions.trunc" should "create date truncation expression" in {
    val result = functions.trunc(functions.col("timestamp"), "month")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "trunc"
      case _ => fail("Expected UnresolvedFunction with 'trunc'")
  }

  "functions.date_trunc" should "create date_trunc expression" in {
    val result = functions.date_trunc("day", functions.col("timestamp"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "date_trunc"
      case _ => fail("Expected UnresolvedFunction with 'date_trunc'")
  }

  // ===========================================================================
  // String Functions Tests
  // ===========================================================================

  "functions.concat_ws" should "create concat with separator expression" in {
    val result = functions.concat_ws(",", functions.col("a"), functions.col("b"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "concat_ws"
        func.arguments.length shouldBe 3 // separator + 2 columns
      case _ => fail("Expected UnresolvedFunction with 'concat_ws'")
  }

  "functions.format_string" should "create format_string expression" in {
    val result = functions.format_string("Hello %s", functions.col("name"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "format_string"
      case _ => fail("Expected UnresolvedFunction with 'format_string'")
  }

  "functions.locate" should "create locate expression" in {
    val result = functions.locate("@", functions.col("email"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "locate"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'locate'")
  }

  "functions.lpad" should "create left padding expression" in {
    val result = functions.lpad(functions.col("str"), 10, " ")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "lpad"
        func.arguments.length shouldBe 3
      case _ => fail("Expected UnresolvedFunction with 'lpad'")
  }

  "functions.rpad" should "create right padding expression" in {
    val result = functions.rpad(functions.col("str"), 10, "0")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "rpad"
      case _ => fail("Expected UnresolvedFunction with 'rpad'")
  }

  "functions.translate" should "create translate expression" in {
    val result = functions.translate(functions.col("text"), "abc", "123")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "translate"
        func.arguments.length shouldBe 3
      case _ => fail("Expected UnresolvedFunction with 'translate'")
  }

  // ===========================================================================
  // Array Functions Tests
  // ===========================================================================

  "functions.array_contains" should "create array_contains expression" in {
    val result = functions.array_contains(functions.col("tags"), "spark")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "array_contains"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'array_contains'")
  }

  "functions.array_distinct" should "create array_distinct expression" in {
    val result = functions.array_distinct(functions.col("items"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "array_distinct"
      case _ => fail("Expected UnresolvedFunction with 'array_distinct'")
  }

  "functions.array_union" should "create array union expression" in {
    val result = functions.array_union(functions.col("arr1"), functions.col("arr2"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "array_union"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'array_union'")
  }

  "functions.array_intersect" should "create array intersect expression" in {
    val result = functions.array_intersect(functions.col("arr1"), functions.col("arr2"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "array_intersect"
      case _ => fail("Expected UnresolvedFunction with 'array_intersect'")
  }

  "functions.array_except" should "create array except expression" in {
    val result = functions.array_except(functions.col("arr1"), functions.col("arr2"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "array_except"
      case _ => fail("Expected UnresolvedFunction with 'array_except'")
  }

  "functions.array_join" should "create array_join expression" in {
    val result = functions.array_join(functions.col("tags"), ", ")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "array_join"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'array_join'")
  }

  "functions.array_max" should "create array_max expression" in {
    val result = functions.array_max(functions.col("values"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "array_max"
      case _ => fail("Expected UnresolvedFunction with 'array_max'")
  }

  "functions.array_min" should "create array_min expression" in {
    val result = functions.array_min(functions.col("values"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "array_min"
      case _ => fail("Expected UnresolvedFunction with 'array_min'")
  }

  "functions.array_position" should "create array_position expression" in {
    val result = functions.array_position(functions.col("items"), "target")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "array_position"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'array_position'")
  }

  "functions.array_remove" should "create array_remove expression" in {
    val result = functions.array_remove(functions.col("items"), "unwanted")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "array_remove"
      case _ => fail("Expected UnresolvedFunction with 'array_remove'")
  }

  "functions.array_sort" should "create array_sort expression" in {
    val result = functions.array_sort(functions.col("items"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "array_sort"
      case _ => fail("Expected UnresolvedFunction with 'array_sort'")
  }

  "functions.flatten" should "create flatten expression" in {
    val result = functions.flatten(functions.col("nested_arrays"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "flatten"
      case _ => fail("Expected UnresolvedFunction with 'flatten'")
  }

  "functions.reverse" should "create reverse expression" in {
    val result = functions.reverse(functions.col("items"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "reverse"
      case _ => fail("Expected UnresolvedFunction with 'reverse'")
  }

  "functions.shuffle" should "create shuffle expression" in {
    val result = functions.shuffle(functions.col("items"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "shuffle"
      case _ => fail("Expected UnresolvedFunction with 'shuffle'")
  }

  "functions.slice" should "create slice expression" in {
    val result = functions.slice(functions.col("items"), 1, 5)

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "slice"
        func.arguments.length shouldBe 3
      case _ => fail("Expected UnresolvedFunction with 'slice'")
  }

  "functions.sort_array" should "create sort_array expression with default order" in {
    val result = functions.sort_array(functions.col("items"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "sort_array"
      case _ => fail("Expected UnresolvedFunction with 'sort_array'")
  }

  "functions.sort_array with order" should "create sort_array expression with specified order" in {
    val result = functions.sort_array(functions.col("items"), asc = false)

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "sort_array"
      case _ => fail("Expected UnresolvedFunction with 'sort_array'")
  }

  "functions.arrays_zip" should "create arrays_zip expression" in {
    val result = functions.arrays_zip(functions.col("arr1"), functions.col("arr2"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "arrays_zip"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'arrays_zip'")
  }

  "functions.arrays_overlap" should "create arrays_overlap expression" in {
    val result = functions.arrays_overlap(functions.col("arr1"), functions.col("arr2"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "arrays_overlap"
      case _ => fail("Expected UnresolvedFunction with 'arrays_overlap'")
  }

  // ===========================================================================
  // Map Functions Tests
  // ===========================================================================

  "functions.map_from_arrays" should "create map_from_arrays expression" in {
    val result = functions.map_from_arrays(functions.col("keys"), functions.col("values"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "map_from_arrays"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'map_from_arrays'")
  }

  "functions.map_keys" should "create map_keys expression" in {
    val result = functions.map_keys(functions.col("map_col"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "map_keys"
      case _ => fail("Expected UnresolvedFunction with 'map_keys'")
  }

  "functions.map_values" should "create map_values expression" in {
    val result = functions.map_values(functions.col("map_col"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "map_values"
      case _ => fail("Expected UnresolvedFunction with 'map_values'")
  }

  "functions.map_contains_key" should "create map_contains_key expression" in {
    val result = functions.map_contains_key(functions.col("map_col"), "target_key")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "map_contains_key"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'map_contains_key'")
  }

  // ===========================================================================
  // Window Functions Tests
  // ===========================================================================

  "functions.lead" should "create lead window function" in {
    val result = functions.lead(functions.col("value"), 1)

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "lead"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'lead'")
  }

  "functions.dense_rank" should "create dense_rank window function" in {
    val result = functions.dense_rank()

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "dense_rank"
      case _ => fail("Expected UnresolvedFunction with 'dense_rank'")
  }

  "functions.ntile" should "create ntile window function" in {
    val result = functions.ntile(4)

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "ntile"
        func.arguments.length shouldBe 1
      case _ => fail("Expected UnresolvedFunction with 'ntile'")
  }

  // ===========================================================================
  // Math Functions Tests
  // ===========================================================================

  "functions.cos" should "create cosine expression" in {
    val result = functions.cos(functions.col("angle"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "cos"
      case _ => fail("Expected UnresolvedFunction with 'cos'")
  }

  "functions.tan" should "create tangent expression" in {
    val result = functions.tan(functions.col("angle"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "tan"
      case _ => fail("Expected UnresolvedFunction with 'tan'")
  }

  "functions.asin" should "create arcsine expression" in {
    val result = functions.asin(functions.col("value"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "asin"
      case _ => fail("Expected UnresolvedFunction with 'asin'")
  }

  "functions.greatest" should "create greatest expression" in {
    val result = functions.greatest(functions.col("a"), functions.col("b"), functions.col("c"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "greatest"
        func.arguments.length shouldBe 3
      case _ => fail("Expected UnresolvedFunction with 'greatest'")
  }

  "functions.least" should "create least expression" in {
    val result = functions.least(functions.col("x"), functions.col("y"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "least"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'least'")
  }

  "functions.hypot" should "create hypotenuse expression" in {
    val result = functions.hypot(functions.col("a"), functions.col("b"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "hypot"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'hypot'")
  }

  "functions.cbrt" should "create cube root expression" in {
    val result = functions.cbrt(functions.col("value"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "cbrt"
      case _ => fail("Expected UnresolvedFunction with 'cbrt'")
  }

  "functions.factorial" should "create factorial expression" in {
    val result = functions.factorial(functions.col("n"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "factorial"
      case _ => fail("Expected UnresolvedFunction with 'factorial'")
  }

  "functions.rand with seed" should "create random expression with seed" in {
    val result = functions.rand(42L)

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "rand"
      case _ => fail("Expected UnresolvedFunction with 'rand'")
  }

  "functions.randn with seed" should "create random normal expression with seed" in {
    val result = functions.randn(42L)

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "randn"
      case _ => fail("Expected UnresolvedFunction with 'randn'")
  }

  // ===========================================================================
  // JSON/Struct Functions Tests
  // ===========================================================================

  "functions.struct" should "create struct expression" in {
    val result = functions.struct(functions.col("a"), functions.col("b"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "struct"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'struct'")
  }

  "functions.get_json_object" should "create get_json_object expression" in {
    val result = functions.get_json_object(functions.col("json_str"), "$.field")

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "get_json_object"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'get_json_object'")
  }

  "functions.to_json" should "create to_json expression" in {
    val result = functions.to_json(functions.col("struct_col"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "to_json"
      case _ => fail("Expected UnresolvedFunction with 'to_json'")
  }

  "functions.from_json" should "create from_json expression" in {
    val result = functions.from_json(functions.col("json_str"), functions.col("schema"))

    result.expr.exprType match
      case Expression.ExprType.UnresolvedFunction(func) =>
        func.functionName shouldBe "from_json"
        func.arguments.length shouldBe 2
      case _ => fail("Expected UnresolvedFunction with 'from_json'")
  }
