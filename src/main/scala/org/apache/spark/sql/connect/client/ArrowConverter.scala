package org.apache.spark.sql.connect.client

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex._
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.spark.sql.Row
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import java.io.ByteArrayInputStream

/**
 * Utility for converting Arrow IPC format to Spark Rows.
 */
object ArrowConverter:

  /**
   * Convert Arrow batch data (IPC format) to a sequence of Rows.
   *
   * @param data the serialized Arrow IPC data
   * @return a sequence of Row objects
   */
  def arrowBatchToRows(data: Array[Byte]): Seq[Row] =
    if data.isEmpty then
      return Seq.empty

    val allocator = new RootAllocator(Long.MaxValue)
    try
      val inputStream = new ByteArrayInputStream(data)
      val reader = new ArrowStreamReader(inputStream, allocator)

      try
        val rows = mutable.ArrayBuffer[Row]()

        // Read the schema
        val root = reader.getVectorSchemaRoot

        // Process all batches in the stream
        while reader.loadNextBatch() do
          val numRows = root.getRowCount
          val fieldVectors = root.getFieldVectors.asScala.toSeq

          // Convert each row
          for rowIndex <- 0 until numRows do
            val values = fieldVectors.map { vector =>
              extractValue(vector, rowIndex)
            }
            rows += Row.fromSeq(values)

        rows.toSeq
      finally
        reader.close()
    catch
      case e: Exception =>
        System.err.println(s"Error parsing Arrow data: ${e.getMessage}")
        e.printStackTrace()
        Seq.empty
    finally
      allocator.close()

  /**
   * Extract a value from an Arrow vector at a specific index.
   *
   * @param vector the Arrow field vector
   * @param index the row index
   * @return the extracted value
   */
  private def extractValue(vector: FieldVector, index: Int): Any =
    if vector.isNull(index) then
      return null

    vector match
      // Integer types
      case v: TinyIntVector => v.get(index)
      case v: SmallIntVector => v.get(index)
      case v: IntVector => v.get(index)
      case v: BigIntVector => v.get(index)

      // Floating point types
      case v: Float4Vector => v.get(index)
      case v: Float8Vector => v.get(index)

      // Boolean
      case v: BitVector => v.get(index) == 1

      // String/Binary
      case v: VarCharVector => v.getObject(index).toString
      case v: VarBinaryVector => v.getObject(index)

      // Date/Time
      case v: DateDayVector =>
        val days = v.get(index)
        new java.sql.Date(days.toLong * 24L * 60L * 60L * 1000L)

      case v: TimeStampMicroVector =>
        val micros = v.get(index)
        new java.sql.Timestamp(micros / 1000L)

      case v: TimeStampMilliVector =>
        val millis = v.get(index)
        new java.sql.Timestamp(millis)

      case v: TimeStampNanoVector =>
        val nanos = v.get(index)
        new java.sql.Timestamp(nanos / 1000000L)

      case v: TimeStampSecVector =>
        val seconds = v.get(index)
        new java.sql.Timestamp(seconds * 1000L)

      // Decimal
      case v: DecimalVector =>
        v.getObject(index)

      case v: Decimal256Vector =>
        v.getObject(index)

      // Complex types - basic support
      case v: ListVector =>
        val list = mutable.ArrayBuffer[Any]()
        val innerVector = v.getDataVector
        val start = v.getElementStartIndex(index)
        val end = v.getElementEndIndex(index)
        for i <- start until end do
          list += extractValue(innerVector, i)
        list.toSeq

      case v: StructVector =>
        val fields = v.getChildrenFromFields.asScala.toSeq
        val values = fields.map(field => extractValue(field, index))
        Row.fromSeq(values)

      case v: MapVector =>
        // Maps are stored as lists of key-value structs in Arrow
        val entries = extractValue(v.getDataVector, index)
        entries match
          case seq: Seq[_] =>
            seq.collect {
              case row: Row if row.size == 2 =>
                row.get(0) -> row.get(1)
            }.toMap
          case _ => Map.empty

      // Fallback for other types
      case v =>
        try
          v.getObject(index)
        catch
          case e: Exception =>
            System.err.println(s"Warning: Could not extract value from ${v.getClass.getSimpleName} at index $index: ${e.getMessage}")
            null
