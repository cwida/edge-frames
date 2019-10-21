package leapfrogTriejoin


import scala.math.{floor, min}
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.A
import org.apache.spark.sql.vectorized.ColumnVector

object ArraySearch {
  def find(values: Array[Long], key: Long, start: Int, end: Int, linearSearchThreshold: Int): Int = {
    assert(0 <= start)
    assert(start < end)

    if (end - start < linearSearchThreshold) {
      linearSearch(values, key, start, end)
    } else {
      binarySearch(values, key, start, end, linearSearchThreshold)
    }
  }

  @inline
  def linearSearch(vector: Array[Long], key: Long, start: Int, end: Int): Int = {
    if (key > vector(end - 1)) { // Key is not in range, the least upper bound is end
      end
    } else {
      var pos = start
      while (vector(pos) < key) {  // key is smaller end - 1, there is no need to check we are not running out of range
        pos += 1
      }
      pos
    }
  }

  @inline
  def binarySearch(vector: Array[Long], key: Long, start: Int, end: Int, linearSearchThreshold: Int): Int = {
    assert(0 <= start)
    assert(start < end)

    var L = start
    var R = end
    var M = -1
    while (L < R - linearSearchThreshold) {
      M = (L + R) >> 1 // x >> 1 === x / 2

      if (vector(M) < key) {
        L = M
      } else {
        R = M
      }
    }

    linearSearch(vector, key, L, R)
  }

}
