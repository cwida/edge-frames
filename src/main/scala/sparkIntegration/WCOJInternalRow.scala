package sparkIntegration

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

/**
  * Performance optimization, InternalRow type that allows to set an array
  * of ints directly, read those and does not support null values.
  */
class WCOJInternalRow(var row: Array[Int]) extends GenericInternalRow {

  override def getInt(offset: Int): Int = row(offset)

  override def isNullAt(ordinal: Int): Boolean = false
}
