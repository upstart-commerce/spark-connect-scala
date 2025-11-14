package org.apache.spark.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.connect.proto.types.{DataType as ProtoDataType}

class DataTypeConverterSpec extends AnyFlatSpec with Matchers:

  // ===========================================================================
  // Primitive Type Conversion Tests
  // ===========================================================================

  "DataTypeConverter.fromProto" should "convert NullType" in {
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Null(ProtoDataType.NULL()))
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe NullType
  }

  "DataTypeConverter.fromProto" should "convert BooleanType" in {
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Boolean(ProtoDataType.Boolean()))
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe BooleanType
  }

  "DataTypeConverter.fromProto" should "convert ByteType" in {
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Byte(ProtoDataType.Byte()))
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe ByteType
  }

  "DataTypeConverter.fromProto" should "convert ShortType" in {
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Short(ProtoDataType.Short()))
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe ShortType
  }

  "DataTypeConverter.fromProto" should "convert IntegerType" in {
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Integer(ProtoDataType.Integer()))
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe IntegerType
  }

  "DataTypeConverter.fromProto" should "convert LongType" in {
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Long(ProtoDataType.Long()))
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe LongType
  }

  "DataTypeConverter.fromProto" should "convert FloatType" in {
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Float(ProtoDataType.Float()))
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe FloatType
  }

  "DataTypeConverter.fromProto" should "convert DoubleType" in {
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Double(ProtoDataType.Double()))
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe DoubleType
  }

  "DataTypeConverter.fromProto" should "convert DecimalType" in {
    val protoDecimal = ProtoDataType.Decimal(
      precision = Some(10),
      scale = Some(2)
    )
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Decimal(protoDecimal))
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe DecimalType(10, 2)
  }

  "DataTypeConverter.fromProto" should "convert DecimalType with defaults" in {
    val protoDecimal = ProtoDataType.Decimal(precision = None, scale = None)
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Decimal(protoDecimal))
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe DecimalType(10, 0)
  }

  "DataTypeConverter.fromProto" should "convert StringType" in {
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.String(ProtoDataType.String()))
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe StringType
  }

  "DataTypeConverter.fromProto" should "convert BinaryType" in {
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Binary(ProtoDataType.Binary()))
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe BinaryType
  }

  "DataTypeConverter.fromProto" should "convert DateType" in {
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Date(ProtoDataType.Date()))
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe DateType
  }

  "DataTypeConverter.fromProto" should "convert TimestampType" in {
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Timestamp(ProtoDataType.Timestamp()))
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe TimestampType
  }

  // ===========================================================================
  // Complex Type Conversion Tests
  // ===========================================================================

  "DataTypeConverter.fromProto" should "convert ArrayType" in {
    val elementType = ProtoDataType(kind = ProtoDataType.Kind.Integer(ProtoDataType.Integer()))
    val protoArray = ProtoDataType.Array(
      elementType = Some(elementType),
      containsNull = true
    )
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Array(protoArray))
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe ArrayType(IntegerType, containsNull = true)
  }

  "DataTypeConverter.fromProto" should "convert ArrayType with containsNull=false" in {
    val elementType = ProtoDataType(kind = ProtoDataType.Kind.String(ProtoDataType.String()))
    val protoArray = ProtoDataType.Array(
      elementType = Some(elementType),
      containsNull = false
    )
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Array(protoArray))
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe ArrayType(StringType, containsNull = false)
  }

  "DataTypeConverter.fromProto" should "convert MapType" in {
    val keyType = ProtoDataType(kind = ProtoDataType.Kind.String(ProtoDataType.String()))
    val valueType = ProtoDataType(kind = ProtoDataType.Kind.Integer(ProtoDataType.Integer()))
    val protoMap = ProtoDataType.Map(
      keyType = Some(keyType),
      valueType = Some(valueType),
      valueContainsNull = true
    )
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Map(protoMap))
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe MapType(StringType, IntegerType, valueContainsNull = true)
  }

  "DataTypeConverter.fromProto" should "convert nested ArrayType" in {
    val innerElementType = ProtoDataType(kind = ProtoDataType.Kind.Integer(ProtoDataType.Integer()))
    val innerArray = ProtoDataType.Array(
      elementType = Some(innerElementType),
      containsNull = false
    )
    val innerArrayType = ProtoDataType(kind = ProtoDataType.Kind.Array(innerArray))

    val outerArray = ProtoDataType.Array(
      elementType = Some(innerArrayType),
      containsNull = true
    )
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Array(outerArray))

    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe ArrayType(ArrayType(IntegerType, containsNull = false), containsNull = true)
  }

  // ===========================================================================
  // StructType Conversion Tests
  // ===========================================================================

  "DataTypeConverter.fromProto" should "convert simple StructType" in {
    val field1Type = ProtoDataType(kind = ProtoDataType.Kind.String(ProtoDataType.String()))
    val field1 = ProtoDataType.StructField(
      name = "name",
      dataType = Some(field1Type),
      nullable = true
    )

    val field2Type = ProtoDataType(kind = ProtoDataType.Kind.Integer(ProtoDataType.Integer()))
    val field2 = ProtoDataType.StructField(
      name = "age",
      dataType = Some(field2Type),
      nullable = false
    )

    val protoStruct = ProtoDataType.Struct(fields = Seq(field1, field2))
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Struct(protoStruct))

    val result = DataTypeConverter.fromProto(protoType)

    result shouldBe a[StructType]
    val structType = result.asInstanceOf[StructType]
    structType.fields.length shouldBe 2
    structType.fields(0).name shouldBe "name"
    structType.fields(0).dataType shouldBe StringType
    structType.fields(0).nullable shouldBe true
    structType.fields(1).name shouldBe "age"
    structType.fields(1).dataType shouldBe IntegerType
    structType.fields(1).nullable shouldBe false
  }

  "DataTypeConverter.fromProto" should "convert nested StructType" in {
    val innerField1Type = ProtoDataType(kind = ProtoDataType.Kind.String(ProtoDataType.String()))
    val innerField1 = ProtoDataType.StructField(
      name = "street",
      dataType = Some(innerField1Type),
      nullable = true
    )

    val innerField2Type = ProtoDataType(kind = ProtoDataType.Kind.String(ProtoDataType.String()))
    val innerField2 = ProtoDataType.StructField(
      name = "city",
      dataType = Some(innerField2Type),
      nullable = true
    )

    val innerStruct = ProtoDataType.Struct(fields = Seq(innerField1, innerField2))
    val innerStructType = ProtoDataType(kind = ProtoDataType.Kind.Struct(innerStruct))

    val outerField1Type = ProtoDataType(kind = ProtoDataType.Kind.String(ProtoDataType.String()))
    val outerField1 = ProtoDataType.StructField(
      name = "name",
      dataType = Some(outerField1Type),
      nullable = true
    )

    val outerField2 = ProtoDataType.StructField(
      name = "address",
      dataType = Some(innerStructType),
      nullable = true
    )

    val outerStruct = ProtoDataType.Struct(fields = Seq(outerField1, outerField2))
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Struct(outerStruct))

    val result = DataTypeConverter.fromProto(protoType)

    result shouldBe a[StructType]
    val structType = result.asInstanceOf[StructType]
    structType.fields.length shouldBe 2
    structType.fields(0).name shouldBe "name"
    structType.fields(1).name shouldBe "address"
    structType.fields(1).dataType shouldBe a[StructType]
  }

  "DataTypeConverter.fromProto" should "convert StructType with array field" in {
    val arrayElementType = ProtoDataType(kind = ProtoDataType.Kind.Integer(ProtoDataType.Integer()))
    val arrayType = ProtoDataType.Array(
      elementType = Some(arrayElementType),
      containsNull = false
    )
    val arrayProtoType = ProtoDataType(kind = ProtoDataType.Kind.Array(arrayType))

    val field1Type = ProtoDataType(kind = ProtoDataType.Kind.String(ProtoDataType.String()))
    val field1 = ProtoDataType.StructField(
      name = "name",
      dataType = Some(field1Type),
      nullable = true
    )

    val field2 = ProtoDataType.StructField(
      name = "scores",
      dataType = Some(arrayProtoType),
      nullable = true
    )

    val protoStruct = ProtoDataType.Struct(fields = Seq(field1, field2))
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Struct(protoStruct))

    val result = DataTypeConverter.fromProto(protoType)

    result shouldBe a[StructType]
    val structType = result.asInstanceOf[StructType]
    structType.fields.length shouldBe 2
    structType.fields(1).name shouldBe "scores"
    structType.fields(1).dataType shouldBe ArrayType(IntegerType, containsNull = false)
  }

  "DataTypeConverter.fromProto" should "convert empty StructType" in {
    val protoStruct = ProtoDataType.Struct(fields = Seq.empty)
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Struct(protoStruct))

    val result = DataTypeConverter.fromProto(protoType)

    result shouldBe a[StructType]
    val structType = result.asInstanceOf[StructType]
    structType.fields shouldBe empty
  }

  "DataTypeConverter.structTypeFromProto" should "convert proto Struct directly" in {
    val field1Type = ProtoDataType(kind = ProtoDataType.Kind.Double(ProtoDataType.Double()))
    val field1 = ProtoDataType.StructField(
      name = "price",
      dataType = Some(field1Type),
      nullable = false
    )

    val protoStruct = ProtoDataType.Struct(fields = Seq(field1))

    val result = DataTypeConverter.structTypeFromProto(protoStruct)

    result shouldBe a[StructType]
    result.fields.length shouldBe 1
    result.fields(0).name shouldBe "price"
    result.fields(0).dataType shouldBe DoubleType
    result.fields(0).nullable shouldBe false
  }

  // ===========================================================================
  // Edge Cases and Error Handling
  // ===========================================================================

  "DataTypeConverter.fromProto" should "handle Empty kind" in {
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Empty)
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe NullType
  }

  "DataTypeConverter.fromProto" should "handle ArrayType with default containsNull" in {
    val elementType = ProtoDataType(kind = ProtoDataType.Kind.String(ProtoDataType.String()))
    val protoArray = ProtoDataType.Array(
      elementType = Some(elementType),
      containsNull = true
    )
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Array(protoArray))
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe ArrayType(StringType, containsNull = true)
  }

  "DataTypeConverter.fromProto" should "handle MapType with default valueContainsNull" in {
    val keyType = ProtoDataType(kind = ProtoDataType.Kind.Integer(ProtoDataType.Integer()))
    val valueType = ProtoDataType(kind = ProtoDataType.Kind.String(ProtoDataType.String()))
    val protoMap = ProtoDataType.Map(
      keyType = Some(keyType),
      valueType = Some(valueType),
      valueContainsNull = true
    )
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Map(protoMap))
    val result = DataTypeConverter.fromProto(protoType)
    result shouldBe MapType(IntegerType, StringType, valueContainsNull = true)
  }

  "DataTypeConverter.fromProto" should "handle StructField with default nullable" in {
    val fieldType = ProtoDataType(kind = ProtoDataType.Kind.Long(ProtoDataType.Long()))
    val field = ProtoDataType.StructField(
      name = "id",
      dataType = Some(fieldType),
      nullable = true  // Default is true in Spark
    )

    val protoStruct = ProtoDataType.Struct(fields = Seq(field))
    val protoType = ProtoDataType(kind = ProtoDataType.Kind.Struct(protoStruct))

    val result = DataTypeConverter.fromProto(protoType)

    val structType = result.asInstanceOf[StructType]
    structType.fields(0).nullable shouldBe true
  }
