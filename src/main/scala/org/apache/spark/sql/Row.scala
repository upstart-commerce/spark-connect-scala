package org.apache.spark.sql

/**
 * Represents a row in a DataFrame.
 *
 * A Row is an ordered collection of fields that can be accessed by index or name.
 * Values in a row can be of any type, including null.
 */
trait Row:
  /**
   * The number of fields in this row.
   */
  def size: Int

  /**
   * Get the value at index i.
   */
  def get(i: Int): Any

  /**
   * Get the value at index i as a specific type.
   */
  def getAs[T](i: Int): T = get(i).asInstanceOf[T]

  /**
   * Get the value by field name.
   */
  def getAs[T](fieldName: String): T

  /**
   * Check if the value at index i is null.
   */
  def isNullAt(i: Int): Boolean = get(i) == null

  /**
   * Get value as Boolean.
   */
  def getBoolean(i: Int): Boolean = getAs[Boolean](i)

  /**
   * Get value as Byte.
   */
  def getByte(i: Int): Byte = getAs[Byte](i)

  /**
   * Get value as Short.
   */
  def getShort(i: Int): Short = getAs[Short](i)

  /**
   * Get value as Int.
   */
  def getInt(i: Int): Int = getAs[Int](i)

  /**
   * Get value as Long.
   */
  def getLong(i: Int): Long = getAs[Long](i)

  /**
   * Get value as Float.
   */
  def getFloat(i: Int): Float = getAs[Float](i)

  /**
   * Get value as Double.
   */
  def getDouble(i: Int): Double = getAs[Double](i)

  /**
   * Get value as String.
   */
  def getString(i: Int): String = getAs[String](i)

  /**
   * Get value as java.sql.Date.
   */
  def getDate(i: Int): java.sql.Date = getAs[java.sql.Date](i)

  /**
   * Get value as java.sql.Timestamp.
   */
  def getTimestamp(i: Int): java.sql.Timestamp = getAs[java.sql.Timestamp](i)

  /**
   * Get all values as a sequence.
   */
  def toSeq: Seq[Any]

  /**
   * Get schema for this row.
   */
  def schema: StructType

object Row:

  /**
   * Create a Row from values.
   */
  def apply(values: Any*): Row =
    GenericRow(values.toIndexedSeq, None)

  /**
   * Create a Row from a sequence of values.
   */
  def fromSeq(values: Seq[Any]): Row =
    GenericRow(values.toIndexedSeq, None)

/**
 * A generic implementation of Row.
 */
private case class GenericRow(
  private val values: IndexedSeq[Any],
  private val schemaOpt: Option[StructType]
) extends Row:

  override def size: Int = values.size

  override def get(i: Int): Any =
    if i < 0 || i >= size then
      throw new IndexOutOfBoundsException(s"Index $i out of bounds for row of size $size")
    values(i)

  override def getAs[T](fieldName: String): T =
    schemaOpt match
      case Some(s) =>
        val index = s.fieldIndex(fieldName)
        getAs[T](index)
      case None =>
        throw new UnsupportedOperationException("Cannot access field by name without schema")

  override def toSeq: Seq[Any] = values.toSeq

  override def schema: StructType =
    schemaOpt.getOrElse(throw new UnsupportedOperationException("Schema not available"))

  override def toString: String =
    s"[${values.mkString(",")}]"
