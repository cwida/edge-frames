package testing

import leapfrogTriejoin.TrieIterator

import scala.collection.mutable

object Utils {

  def traverseTrieIterator(iter: TrieIterator): Seq[(Int, Int)] = {
    if (iter.atEnd) {
      return List()
    }
    var ret: mutable.MutableList[(Int, Int)] = mutable.MutableList()
    iter.open()
    do {
      val outer: Int = iter.key
      iter.open()
      do {
        ret += ((outer, iter.key))
        iter.next()
      } while (!iter.atEnd)
      iter.up()
      iter.next()
    } while (!iter.atEnd)
    ret
  }


}
