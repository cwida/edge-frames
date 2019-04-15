package leapfrogTriejoin

import scala.math.{min, floor}
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.A
import org.apache.spark.sql.vectorized.ColumnVector

object GaloppingSearch {


  def find(values: ColumnVector, key: Int, start: Int, end: Int): Int = {
    if (end == 0) {
      return 0
    }

    var bound = 1
    while (bound < end && values.getInt(bound) < key) {
      bound *= 2
    }
    binarySearch(values, key, bound / 2, min(bound + 1, end))
  }

  def binarySearch(vector: ColumnVector, key: Int, start: Int, end: Int): Int = {
    var L = start
    var R = end
    while (L <= R) {
      var M: Int = (L + R) / 2
      if (vector.getInt(M) < key) {
        L = M + 1
      } else if (vector.getInt(M) > key ) {
        R = M - 1
      } else {
        return M
      }
    }
    L
  }

}
