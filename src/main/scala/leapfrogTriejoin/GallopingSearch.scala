package leapfrogTriejoin

import scala.math.{min, floor}
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.A
import org.apache.spark.sql.vectorized.ColumnVector

object GallopingSearch {

  private val LINEAR_SEARCH_THRESHOLD = 60

  // TODO could be used with summary
  def find(values: Array[Int], key: Int, start: Int, end: Int): Int = {
    assert(end != 0)
    assert(start < end)

    // TODO shortcut to linear search alrday here

    var bound = Math.max(start, 1)
    while (bound < end && values(bound) < key) {
      bound *= 2
    }
    binarySearch(values, key, Math.max(start, bound / 2), min(bound + 1, end))
  }

  def linearSearch(vector: Array[Int], key: Int, start: Int, end: Int): Int = {
    var pos = start
    while (pos < end && vector(pos) < key) {
      pos += 1
    }
    pos
  }

  // TODO write more unit tests about this
  // TODO operate direclty on arrays, should be faster (no virtual function calls)
  def binarySearch(vector: Array[Int], key: Int, start: Int, end: Int): Int = {
    assert(0 <= start)
    assert(start < end)

    var L = start
    var R = end
    var M = -1
    while (L < R - LINEAR_SEARCH_THRESHOLD) {
      M = (L + R) >> 1  // x >> 1 === x / 2

      if (vector(M) < key) {
        L = M + 1
      } else if (vector(M) >= key) {
        R = M - 1
      }
    }

    if (L < R) {
      linearSearch(vector, key, L, Math.min(R + 1, end))
    } else {
      if (vector(M) == key) { // 105 --> 99
        if (start < M && vector(M) == key) {
          M -= 1
        }
        M
      } else {
        Math.min(if (L < end && vector(L) < key) {
          L + 1
        } else {
          L
        }, end)
      }
    }
  }

}
