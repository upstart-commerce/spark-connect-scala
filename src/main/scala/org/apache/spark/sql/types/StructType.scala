package org.apache.spark.sql

/**
 * A field in a StructType.
 *
 * @param name the field name
 * @param dataType the field data type
 * @param nullable whether the field can be null
 * @param metadata metadata for this field
 */
case class StructField(
  name: String,
  dataType: DataType,
  nullable: Boolean = true,
  metadata: Map[String, Any] = Map.empty
)

/**
 * The data type representing structured data (a struct).
 *
 * @param fields the fields in this struct
 */
case class StructType(fields: Seq[StructField]) extends DataType:

  /**
   * Get the field at the given index.
   */
  def apply(i: Int): StructField = fields(i)

  /**
   * Get the field with the given name.
   */
  def apply(name: String): StructField =
    fields.find(_.name == name).getOrElse {
      throw new IllegalArgumentException(s"Field $name does not exist")
    }

  /**
   * Get the index of a field by name.
   */
  def fieldIndex(name: String): Int =
    fields.indexWhere(_.name == name) match
      case -1 => throw new IllegalArgumentException(s"Field $name does not exist")
      case i => i

  /**
   * Get field names.
   */
  def fieldNames: Seq[String] = fields.map(_.name)

  /**
   * Get the number of fields.
   */
  def size: Int = fields.size

  /**
   * Check if this struct contains a field with the given name.
   */
  def contains(name: String): Boolean =
    fields.exists(_.name == name)

  /**
   * Add a field to this struct.
   */
  def add(field: StructField): StructType =
    StructType(fields :+ field)

  /**
   * Add a field to this struct.
   */
  def add(name: String, dataType: DataType): StructType =
    add(StructField(name, dataType))

  /**
   * Add a field to this struct.
   */
  def add(name: String, dataType: DataType, nullable: Boolean): StructType =
    add(StructField(name, dataType, nullable))

  override def toString: String =
    s"StructType(${fields.mkString(",")})"

object StructType:

  /**
   * Create an empty StructType.
   */
  def empty: StructType = StructType(Seq.empty)

  /**
   * Create a StructType from fields.
   */
  def fromFields(fields: StructField*): StructType =
    StructType(fields.toSeq)

/**
 * Base trait for all data types.
 */
sealed trait DataType:
  override def toString: String = this.getClass.getSimpleName.replace("$", "")

// Numeric types
case object ByteType extends DataType
case object ShortType extends DataType
case object IntegerType extends DataType
case object LongType extends DataType
case object FloatType extends DataType
case object DoubleType extends DataType
case class DecimalType(precision: Int, scale: Int) extends DataType

// String and binary types
case object StringType extends DataType
case object BinaryType extends DataType

// Boolean type
case object BooleanType extends DataType

// Date and timestamp types
case object DateType extends DataType
case object TimestampType extends DataType

// Complex types
case class ArrayType(elementType: DataType, containsNull: Boolean = true) extends DataType
case class MapType(keyType: DataType, valueType: DataType, valueContainsNull: Boolean = true) extends DataType

// Null type
case object NullType extends DataType
