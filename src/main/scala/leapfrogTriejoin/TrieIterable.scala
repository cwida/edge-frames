package leapfrogTriejoin

trait TrieIterable extends Iterable[(Int, Int)] {

  def trieIterator: TrieIterator

}
