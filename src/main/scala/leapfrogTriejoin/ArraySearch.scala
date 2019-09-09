package leapfrogTriejoin

import scala.math.{min, floor}
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.A
import org.apache.spark.sql.vectorized.ColumnVector

object ArraySearch {

  private val LINEAR_SEARCH_THRESHOLD = 60

  def find(values: Array[Long], key: Long, start: Int, end: Int): Int = {
    assert(0 <= start)
    assert(start < end)

    if (end - start < LINEAR_SEARCH_THRESHOLD) {
      linearSearch(values, key, start, end)
    } else {
      binarySearch(values, key, start, end)
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
  def binarySearch(vector: Array[Long], key: Long, start: Int, end: Int): Int = {
    assert(0 <= start)
    assert(start < end)

    var L = start
    var R = end
    var M = -1
    while (L < R - LINEAR_SEARCH_THRESHOLD) {
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
