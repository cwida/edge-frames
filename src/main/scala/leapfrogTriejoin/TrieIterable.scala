package leapfrogTriejoin

import org.apache.spark.sql.catalyst.InternalRow

trait TrieIterable extends Iterable[InternalRow] with Serializable {

  def trieIterator: TrieIterator

  /**
    *
    * @return Memory usage of this Iterable in bytes.
    */
  def memoryUsage: Long

}
