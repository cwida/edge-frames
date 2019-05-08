package leapfrogTriejoin

/**
  * An OnHeapColumnVector variant that exposes it's internal array.
  */
class OpenArrayColumnVector(capacity: Int) extends WritableIntColumnVector(capacity) {
  var intData = new Array[Int](capacity)

  override def getInt(i: Int): Int = intData(i)

  override def reserveInternal(newCapacity: Int): Unit = {
    if (intData.length < newCapacity) {
      val newData = new Array[Int](newCapacity)
      System.arraycopy(intData, 0, newData, 0, capacity)
      intData = newData
    }
  }

  override def putInt(rowId: Int, value: Int): Unit = {
    intData(rowId) = value
  }
}
