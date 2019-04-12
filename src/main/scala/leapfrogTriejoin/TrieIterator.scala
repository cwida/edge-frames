package leapfrogTriejoin

trait TrieIterator extends LinearIterator {

  def open(): Unit // O(log N)

  def up(): Unit // O(log N)

}
