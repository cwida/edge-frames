package leapfrogTriejoin

trait LinearIterator extends Cloneable {

  def key: Long // O (1)

  def next(): Unit  // O(log N)
    /**
      * Moreover, if m keys are visited in as- cending order, the amortized complexity is required to be O(1 + log(N/m)), which can be accomplished using standard data structures (notably, balanced trees such as B-trees.2)
      */

  def atEnd: Boolean

  def seek(key: Long): Boolean  // O(log N)

  /**
    *
    * @return A size estimate which can be too big but never too small.
    */
  def estimateSize: Int = Integer.MAX_VALUE

  override def clone(): AnyRef = ???

}
