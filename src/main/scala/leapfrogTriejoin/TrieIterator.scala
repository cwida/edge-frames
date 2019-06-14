package leapfrogTriejoin

trait TrieIterator extends LinearIterator {

  def open(): Unit // O(log N)

  def up(): Unit // O(log N)

  // Allows iterator to map keys in place, e.g. for CSR form index space to graph key space
  def translate(keys: Array[Long]): Array[Long]

  /**
    *
    * @return
    */
  def getDepth: Int
}
