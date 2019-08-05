package partitioning

import leapfrogTriejoin.TrieIterator

class TrieIteratorDecorator(val trieIter: TrieIterator) extends TrieIterator {
  def co() = ???
  override def open(): Unit = trieIter.open()

  override def up(): Unit = trieIter.up()

  override def translate(keys: Array[Long]): Array[Long] = trieIter.translate(keys)

  override def getDepth: Int = trieIter.getDepth

  override def key: Long = trieIter.key

  override def next(): Unit = trieIter.next()

  override def atEnd: Boolean = trieIter.atEnd

  override def seek(key: Long): Boolean = trieIter.seek(key)
}
