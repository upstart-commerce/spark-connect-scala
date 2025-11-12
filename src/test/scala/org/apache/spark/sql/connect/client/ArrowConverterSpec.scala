package org.apache.spark.sql.connect.client

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.spark.sql.Row
import scala.jdk.CollectionConverters._
import java.io.ByteArrayOutputStream

class ArrowConverterSpec extends AnyFlatSpec with Matchers:

  "ArrowConverter.arrowBatchToRows" should "handle empty data" in {
    val rows = ArrowConverter.arrowBatchToRows(Array.empty[Byte])
    rows shouldBe empty
  }

  "ArrowConverter.arrowBatchToRows" should "convert integer vector to rows" in {
    val allocator = new RootAllocator(Long.MaxValue)
    var root: VectorSchemaRoot = null
    try
      val intVector = new IntVector("id", allocator)
      intVector.allocateNew(3)
      intVector.set(0, 100)
      intVector.set(1, 200)
      intVector.set(2, 300)
      intVector.setValueCount(3)

      root = new VectorSchemaRoot(Seq(intVector.getField).asJava, Seq(intVector).asJava, 3)

      val output = new ByteArrayOutputStream()
      val writer = new ArrowStreamWriter(root, null, output)
      writer.writeBatch()
      writer.close()

      val data = output.toByteArray
      val rows = ArrowConverter.arrowBatchToRows(data)

      rows.length shouldBe 3
      rows(0).getInt(0) shouldBe 100
      rows(1).getInt(0) shouldBe 200
      rows(2).getInt(0) shouldBe 300
    finally
      if root != null then root.close()
      allocator.close()
  }

  "ArrowConverter.arrowBatchToRows" should "convert multiple column types" in {
    val allocator = new RootAllocator(Long.MaxValue)
    var root: VectorSchemaRoot = null
    try
      // Create vectors for different types
      val intVector = new IntVector("id", allocator)
      intVector.allocateNew(2)
      intVector.set(0, 1)
      intVector.set(1, 2)
      intVector.setValueCount(2)

      val bigIntVector = new BigIntVector("value", allocator)
      bigIntVector.allocateNew(2)
      bigIntVector.set(0, 1000L)
      bigIntVector.set(1, 2000L)
      bigIntVector.setValueCount(2)

      val float8Vector = new Float8Vector("score", allocator)
      float8Vector.allocateNew(2)
      float8Vector.set(0, 3.14)
      float8Vector.set(1, 2.71)
      float8Vector.setValueCount(2)

      val varCharVector = new VarCharVector("name", allocator)
      varCharVector.allocateNew()
      varCharVector.set(0, "Alice".getBytes)
      varCharVector.set(1, "Bob".getBytes)
      varCharVector.setValueCount(2)

      val bitVector = new BitVector("active", allocator)
      bitVector.allocateNew(2)
      bitVector.set(0, 1)
      bitVector.set(1, 0)
      bitVector.setValueCount(2)

      val fields = Seq(
        intVector.getField,
        bigIntVector.getField,
        float8Vector.getField,
        varCharVector.getField,
        bitVector.getField
      ).asJava

      val vectors = Seq[FieldVector](
        intVector,
        bigIntVector,
        float8Vector,
        varCharVector,
        bitVector
      ).asJava

      root = new VectorSchemaRoot(fields, vectors, 2)

      val output = new ByteArrayOutputStream()
      val writer = new ArrowStreamWriter(root, null, output)
      writer.writeBatch()
      writer.close()

      val data = output.toByteArray
      val rows = ArrowConverter.arrowBatchToRows(data)

      rows.length shouldBe 2

      // Check first row
      rows(0).getInt(0) shouldBe 1
      rows(0).getLong(1) shouldBe 1000L
      rows(0).getDouble(2) shouldBe 3.14
      rows(0).getString(3) shouldBe "Alice"
      rows(0).getBoolean(4) shouldBe true

      // Check second row
      rows(1).getInt(0) shouldBe 2
      rows(1).getLong(1) shouldBe 2000L
      rows(1).getDouble(2) shouldBe 2.71
      rows(1).getString(3) shouldBe "Bob"
      rows(1).getBoolean(4) shouldBe false
    finally
      if root != null then root.close()
      allocator.close()
  }

  "ArrowConverter.arrowBatchToRows" should "handle null values" in {
    val allocator = new RootAllocator(Long.MaxValue)
    var root: VectorSchemaRoot = null
    try
      val intVector = new IntVector("nullable_id", allocator)
      intVector.allocateNew(3)
      intVector.set(0, 100)
      intVector.setNull(1)
      intVector.set(2, 300)
      intVector.setValueCount(3)

      root = new VectorSchemaRoot(Seq(intVector.getField).asJava, Seq(intVector).asJava, 3)

      val output = new ByteArrayOutputStream()
      val writer = new ArrowStreamWriter(root, null, output)
      writer.writeBatch()
      writer.close()

      val data = output.toByteArray
      val rows = ArrowConverter.arrowBatchToRows(data)

      rows.length shouldBe 3
      rows(0).getInt(0) shouldBe 100
      rows(1).isNullAt(0) shouldBe true
      rows(2).getInt(0) shouldBe 300
    finally
      if root != null then root.close()
      allocator.close()
  }

  "ArrowConverter.arrowBatchToRows" should "handle small integers" in {
    val allocator = new RootAllocator(Long.MaxValue)
    var root: VectorSchemaRoot = null
    try
      val tinyIntVector = new TinyIntVector("byte_col", allocator)
      tinyIntVector.allocateNew(2)
      tinyIntVector.set(0, 10.toByte)
      tinyIntVector.set(1, 20.toByte)
      tinyIntVector.setValueCount(2)

      val smallIntVector = new SmallIntVector("short_col", allocator)
      smallIntVector.allocateNew(2)
      smallIntVector.set(0, 1000.toShort)
      smallIntVector.set(1, 2000.toShort)
      smallIntVector.setValueCount(2)

      val fields = Seq(tinyIntVector.getField, smallIntVector.getField).asJava
      val vectors = Seq[FieldVector](tinyIntVector, smallIntVector).asJava

      root = new VectorSchemaRoot(fields, vectors, 2)

      val output = new ByteArrayOutputStream()
      val writer = new ArrowStreamWriter(root, null, output)
      writer.writeBatch()
      writer.close()

      val data = output.toByteArray
      val rows = ArrowConverter.arrowBatchToRows(data)

      rows.length shouldBe 2
      rows(0).getByte(0) shouldBe 10.toByte
      rows(0).getShort(1) shouldBe 1000.toShort
      rows(1).getByte(0) shouldBe 20.toByte
      rows(1).getShort(1) shouldBe 2000.toShort
    finally
      if root != null then root.close()
      allocator.close()
  }

  "ArrowConverter.arrowBatchToRows" should "handle float values" in {
    val allocator = new RootAllocator(Long.MaxValue)
    var root: VectorSchemaRoot = null
    try
      val float4Vector = new Float4Vector("float_col", allocator)
      float4Vector.allocateNew(2)
      float4Vector.set(0, 1.5f)
      float4Vector.set(1, 2.5f)
      float4Vector.setValueCount(2)

      root = new VectorSchemaRoot(Seq(float4Vector.getField).asJava, Seq(float4Vector).asJava, 2)

      val output = new ByteArrayOutputStream()
      val writer = new ArrowStreamWriter(root, null, output)
      writer.writeBatch()
      writer.close()

      val data = output.toByteArray
      val rows = ArrowConverter.arrowBatchToRows(data)

      rows.length shouldBe 2
      rows(0).getFloat(0) shouldBe 1.5f
      rows(1).getFloat(0) shouldBe 2.5f
    finally
      if root != null then root.close()
      allocator.close()
  }

  "ArrowConverter.arrowBatchToRows" should "handle timestamp values" in {
    val allocator = new RootAllocator(Long.MaxValue)
    var root: VectorSchemaRoot = null
    try
      val timestampVector = new TimeStampMilliVector("timestamp_col", allocator)
      timestampVector.allocateNew(2)
      timestampVector.set(0, 1609459200000L) // 2021-01-01 00:00:00 UTC
      timestampVector.set(1, 1640995200000L) // 2022-01-01 00:00:00 UTC
      timestampVector.setValueCount(2)

      root = new VectorSchemaRoot(Seq(timestampVector.getField).asJava, Seq(timestampVector).asJava, 2)

      val output = new ByteArrayOutputStream()
      val writer = new ArrowStreamWriter(root, null, output)
      writer.writeBatch()
      writer.close()

      val data = output.toByteArray
      val rows = ArrowConverter.arrowBatchToRows(data)

      rows.length shouldBe 2
      rows(0).getTimestamp(0).getTime shouldBe 1609459200000L
      rows(1).getTimestamp(0).getTime shouldBe 1640995200000L
    finally
      if root != null then root.close()
      allocator.close()
  }

  "ArrowConverter.arrowBatchToRows" should "handle date values" in {
    val allocator = new RootAllocator(Long.MaxValue)
    var root: VectorSchemaRoot = null
    try
      val dateVector = new DateDayVector("date_col", allocator)
      dateVector.allocateNew(2)
      dateVector.set(0, 18628) // Days since epoch
      dateVector.set(1, 18993)
      dateVector.setValueCount(2)

      root = new VectorSchemaRoot(Seq(dateVector.getField).asJava, Seq(dateVector).asJava, 2)

      val output = new ByteArrayOutputStream()
      val writer = new ArrowStreamWriter(root, null, output)
      writer.writeBatch()
      writer.close()

      val data = output.toByteArray
      val rows = ArrowConverter.arrowBatchToRows(data)

      rows.length shouldBe 2
      rows(0).isNullAt(0) shouldBe false
      rows(1).isNullAt(0) shouldBe false
    finally
      if root != null then root.close()
      allocator.close()
  }
