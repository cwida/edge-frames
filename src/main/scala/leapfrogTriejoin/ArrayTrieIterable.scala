package leapfrogTriejoin

class ArrayTrieIterable(val iter: Iterator[(Int, Int)]) extends TrieIterable {
  val tuples: Array[(Int, Int)] = iter.toArray

  def trieIterator: TrieIterator = {
    new TreeTrieIterator(tuples)  // TODO change to real implementation
  }

  class TrieIteratorImpl(val tuples: Array[(Int, Int)]) extends TrieIterator {
    private var position = 0

    override def open(): Unit = {
      ???
    }

    override def up(): Unit = ???

    override def key: Int = tuples(position)._1

    override def next(): Unit = position += 1


    override def atEnd: Boolean = ???

    override def seek(key: Int): Unit = ???
  }

  override def iterator: Iterator[(Int, Int)] = {
    tuples.iterator
  }
}
