package leapfrogTriejoin

/**
  * An OnHeapColumnVector variant that exposes it's internal array.
  */
class OpenArrayColumnVector(capacity: Int) extends WritableLongColumnVector(capacity) {
  var longData = new Array[Long](capacity)

  override def getLong(i: Int): Long = longData(i)

  override def reserveInternal(newCapacity: Int): Unit = {
    if (longData.length < newCapacity) {
      val newData = new Array[Long](newCapacity)
      System.arraycopy(longData, 0, newData, 0, capacity)
      longData = newData
    }
  }

  override def putLong(rowId: Int, value: Long): Unit = {
    longData(rowId) = value
  }
}
