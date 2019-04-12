package leapfrogTriejoin

trait LinearIterator {

  def key: Int // O (1)

  def next(): Unit  // O(log N)
    /**
      * Moreover, if m keys are visited in as- cending order, the amortized complexity is required to be O(1 + log(N/m)), which can be accomplished using standard data structures (notably, balanced trees such as B-trees.2)
      */

  def atEnd: Boolean

  def seek(key: Int): Unit  // O(log N)

}
