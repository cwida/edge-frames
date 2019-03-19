package leapfrogTriejoin

import scala.collection.immutable.TreeMap

class LinearIterator(values: Array[(Int, Int)]) extends UnaryIterator {
  var map = new TreeMap[(Int, Int), Int]()  // TODO do I want a mutable tree map?

  for ((t, i) <- values.zipWithIndex) {
    map = map.updated(t, i)
  }



  override def key: Int = {
    ???
  }

  override def next(): Unit = ???

  /**
    * Moreover, if m keys are visited in as- cending order, the amortized complexity is required to be O(1 + log(N/m)), which can be accomplished using standard data structures (notably, balanced trees such as B-trees.2)
    */
  override def atEnd: Boolean = ???

  override def seek(key: Int): Unit = ???
}
