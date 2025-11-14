package org.apache.spark.sql

import org.apache.spark.connect.proto.types.{DataType as ProtoDataType}
import scala.jdk.CollectionConverters.*

/**
 * Helper object for converting between protocol buffer DataType and Scala DataType.
 */
private[sql] object DataTypeConverter:

  /**
   * Convert a proto DataType to a Scala DataType.
   *
   * @param protoType the proto DataType
   * @return the corresponding Scala DataType
   */
  def fromProto(protoType: ProtoDataType): DataType =
    protoType.kind match
      case ProtoDataType.Kind.Null(_) => NullType

      case ProtoDataType.Kind.Binary(_) => BinaryType

      case ProtoDataType.Kind.Boolean(_) => BooleanType

      case ProtoDataType.Kind.Byte(_) => ByteType
      case ProtoDataType.Kind.Short(_) => ShortType
      case ProtoDataType.Kind.Integer(_) => IntegerType
      case ProtoDataType.Kind.Long(_) => LongType

      case ProtoDataType.Kind.Float(_) => FloatType
      case ProtoDataType.Kind.Double(_) => DoubleType
      case ProtoDataType.Kind.Decimal(d) =>
        DecimalType(
          precision = d.precision.getOrElse(10),
          scale = d.scale.getOrElse(0)
        )

      case ProtoDataType.Kind.String(_) => StringType

      case ProtoDataType.Kind.Date(_) => DateType
      case ProtoDataType.Kind.Timestamp(_) => TimestampType

      case ProtoDataType.Kind.Array(a) =>
        ArrayType(
          elementType = fromProto(a.elementType.get),
          containsNull = a.containsNull
        )

      case ProtoDataType.Kind.Struct(s) =>
        val fields = s.fields.map { protoField =>
          StructField(
            name = protoField.name,
            dataType = fromProto(protoField.dataType.get),
            nullable = protoField.nullable,
            metadata = Map.empty // Simplified - ignore metadata for now
          )
        }
        StructType(fields)

      case ProtoDataType.Kind.Map(m) =>
        MapType(
          keyType = fromProto(m.keyType.get),
          valueType = fromProto(m.valueType.get),
          valueContainsNull = m.valueContainsNull
        )

      case ProtoDataType.Kind.Empty =>
        // Unspecified or unknown type
        NullType

      case _ =>
        throw new IllegalArgumentException(s"Unsupported DataType: ${protoType.kind}")

  /**
   * Convert a proto StructType to a Scala StructType.
   *
   * @param protoStruct the proto Struct
   * @return the corresponding Scala StructType
   */
  def structTypeFromProto(protoStruct: ProtoDataType.Struct): StructType =
    val fields = protoStruct.fields.map { protoField =>
      StructField(
        name = protoField.name,
        dataType = fromProto(protoField.dataType.get),
        nullable = protoField.nullable,
        metadata = Map.empty
      )
    }
    StructType(fields)
