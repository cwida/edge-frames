package leapfrogTriejoin

import scala.math.{min, floor}
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.A
import org.apache.spark.sql.vectorized.ColumnVector

object GaloppingSearch {


  def find(values: ColumnVector, key: Int, start: Int, end: Int): Int = {
    assert(end != 0)
    assert(start < end)

    var bound = Math.max(start, 1)
    while (bound < end && values.getInt(bound) < key) {
      bound *= 2
    }
    binarySearch(values, key, Math.max(start, bound / 2), min(bound + 1, end))
  }

  def binarySearch(vector: ColumnVector, key: Int, start: Int, end: Int): Int = {
    assert(0 <= start)
    assert(start < end)

    var L = start
    var R = end
    while (L <= R) {
      var M: Int = (L + R) / 2
      if (vector.getInt(M) < key) {
        L = M + 1
      } else if (vector.getInt(M) > key) {
        R = M - 1
      } else {
        while (start < M && vector.getInt(M - 1) == key) {
          M -= 1
        }
        return M
      }
    }
    while (start < L && vector.getInt(L - 1) == key) {
      L -= 1
    }
    Math.min(L, end)
  }

}
