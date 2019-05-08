package leapfrogTriejoin

import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.unsafe.types.UTF8String

/**
  * Abstract stub that allows to only implement the method necessary for an ColumnVector for IntegerType
  */
abstract class WritableIntColumnVector(capacity: Int) extends WritableColumnVector(capacity, IntegerType) {
  override def getDictId(i: Int): Int = {
    ???
  }

  override def putNotNull(i: Int): Unit = ???

  override def putNull(i: Int): Unit = ???

  override def putNulls(i: Int, i1: Int): Unit = ???

  override def putNotNulls(i: Int, i1: Int): Unit = ???

  override def putBoolean(i: Int, b: Boolean): Unit = ???

  override def putBooleans(i: Int, i1: Int, b: Boolean): Unit = ???

  override def putByte(i: Int, b: Byte): Unit = ???

  override def putBytes(i: Int, i1: Int, b: Byte): Unit = ???

  override def putBytes(i: Int, i1: Int, bytes: Array[Byte], i2: Int): Unit = ???

  override def putShort(i: Int, i1: Short): Unit = ???

  override def putShorts(i: Int, i1: Int, i2: Short): Unit = ???

  override def putShorts(i: Int, i1: Int, shorts: Array[Short], i2: Int): Unit = ???

  override def putShorts(i: Int, i1: Int, bytes: Array[Byte], i2: Int): Unit = ???

  override def putInts(i: Int, i1: Int, i2: Int): Unit = ???

  override def putInts(i: Int, i1: Int, ints: Array[Int], i2: Int): Unit = ???

  override def putInts(i: Int, i1: Int, bytes: Array[Byte], i2: Int): Unit = ???

  override def putIntsLittleEndian(i: Int, i1: Int, bytes: Array[Byte], i2: Int): Unit = ???

  override def putLong(i: Int, l: Long): Unit = ???

  override def putLongs(i: Int, i1: Int, l: Long): Unit = ???

  override def putLongs(i: Int, i1: Int, longs: Array[Long], i2: Int): Unit = ???

  override def putLongs(i: Int, i1: Int, bytes: Array[Byte], i2: Int): Unit = ???

  override def putLongsLittleEndian(i: Int, i1: Int, bytes: Array[Byte], i2: Int): Unit = ???

  override def putFloat(i: Int, v: Float): Unit = ???

  override def putFloats(i: Int, i1: Int, v: Float): Unit = ???

  override def putFloats(i: Int, i1: Int, floats: Array[Float], i2: Int): Unit = ???

  override def putFloats(i: Int, i1: Int, bytes: Array[Byte], i2: Int): Unit = ???

  override def putDouble(i: Int, v: Double): Unit = ???

  override def putDoubles(i: Int, i1: Int, v: Double): Unit = ???

  override def putDoubles(i: Int, i1: Int, doubles: Array[Double], i2: Int): Unit = ???

  override def putDoubles(i: Int, i1: Int, bytes: Array[Byte], i2: Int): Unit = ???

  override def putArray(i: Int, i1: Int, i2: Int): Unit = ???

  override def putByteArray(i: Int, bytes: Array[Byte], i1: Int, i2: Int): Int = ???

  override def getBytesAsUTF8String(i: Int, i1: Int): UTF8String = ???

  override def getArrayLength(i: Int): Int = ???

  override def getArrayOffset(i: Int): Int = ???

  override def reserveNewColumn(i: Int, dataType: DataType): WritableColumnVector = ???

  override def isNullAt(i: Int): Boolean = ???

  override def getBoolean(i: Int): Boolean = ???

  override def getByte(i: Int): Byte = ???

  override def getShort(i: Int): Short = ???

  override def getLong(i: Int): Long = ???

  override def getFloat(i: Int): Float = ???

  override def getDouble(i: Int): Double = ???
}
