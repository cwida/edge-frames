package leapfrogTriejoin

import org.apache.spark.sql.catalyst.InternalRow

class CSRTrieIterable(iter: Iterator[InternalRow]) extends TrieIterable {
  val arrayTrieIterable = new ArrayTrieIterable(iter)

  override def trieIterator: TrieIterator = arrayTrieIterable.trieIterator


  override def iterator: Iterator[InternalRow] = arrayTrieIterable.iterator

  override def memoryUsage: Long = arrayTrieIterable.memoryUsage
}
