package leapfrogTriejoin;

import org.apache.spark.sql.vectorized.ColumnVector;

// Unused
public class GallopingSearchJava {
  public static int find(ColumnVector values, int key, int start, int end) {
    assert (end != 0);
    assert (start < end);

    int bound = Math.max(start, 1);
    while (bound < end && values.getInt(bound) < key) {
      bound *= 2;
    }
    return binarySearch(values, key, Math.max(start, bound / 2), Math.min(bound + 1, end));
  }

  public static int binarySearch(ColumnVector vector, int key, int start, int end) {
    assert (0 <= start);
    assert (start < end);

    int L = start;
    int R = end;
    while (L < R) {
      int M = (L + R) / 2;
      if (vector.getInt(M) < key) {
        L = M + 1;
      } else if (vector.getInt(M) > key) {
        R = M - 1;
      } else {
        while (start < M && vector.getInt(M - 1) == key) {
          M -= 1;
        }
        return M;
      }
    }
    while (start < L && vector.getInt(L - 1) == key) {
      L -= 1;
    }

    return Math.min((L < end && vector.getInt(L) < key) ? L + 1 : L, end);
  }
}
