package leapfrogTriejoin

import scala.math.{min, floor}
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.A
import org.apache.spark.sql.vectorized.ColumnVector

object GallopingSearch {

  private val LINEAR_SEARCH_THRESHOLD = 60

  // TODO could be used with summary
  def find(values: Array[Int], key: Int, start: Int, end: Int): Int = {
    assert(0 <= start)
    assert(start < end)

    if (end - start < LINEAR_SEARCH_THRESHOLD) {
      linearSearch(values, key, start, end)
    } else {
      var bound = Math.max(start, 1)
      while (bound < end && values(bound) < key) {
        bound *= 2
      }
      binarySearch(values, key, Math.max(start, bound / 2), min(bound + 1, end))
    }
  }

  @inline
  def linearSearch(vector: Array[Int], key: Int, start: Int, end: Int): Int = {
    var pos = start
    // TODO check end only once
    while (pos < end && vector(pos) < key) {
      pos += 1
    }
    pos
  }

  // TODO write more unit tests about this
  @inline
  def binarySearch(vector: Array[Int], key: Int, start: Int, end: Int): Int = {
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
