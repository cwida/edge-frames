package leapfrogTriejoin

import org.apache.spark.sql.catalyst.InternalRow

trait TrieIterable extends Iterable[InternalRow] {

  def trieIterator: TrieIterator

  def memoryUsage: Long

}
