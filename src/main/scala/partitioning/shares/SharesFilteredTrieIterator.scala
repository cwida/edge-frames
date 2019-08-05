package partitioning.shares

import leapfrogTriejoin.TrieIterator
import org.slf4j.LoggerFactory
import partitioning.TrieIteratorDecorator

class SharesFilteredTrieIterator(trieIter: TrieIterator, partition: Int, dimensions: Array[Int], hypercube: Hypercube) extends TrieIteratorDecorator(trieIter) {
  override def co() = ???
  private val logger = LoggerFactory.getLogger(classOf[SharesFilteredTrieIterator])

  private[this] val coordinate: Array[Int] = hypercube.getCoordinate(partition)
//  logger.error("coordinate " + coordinate.mkString(", ") + "partition " + partition)

  private[this] val hashes: Array[Hash] = hypercube.dimensionSizes.zipWithIndex.map {
    case (s, i) => {
      new Hash(i, s)
    }
  }

  // TODO factor out current dimension, introduce own depth variable, current hash
//  override def open(): Unit = {
//    trieIter.open()
//    while (isFiltered(trieIter.key.toInt) && !trieIter.atEnd) {
//      trieIter.next()
//    }
//  }
//
//  override def next(): Unit = {
//    trieIter.next()
//    while (isFiltered(trieIter.key.toInt) && !trieIter.atEnd) {
//      trieIter.next()
//    }
//  }
//
//
//    override def seek(key: Long): Boolean = {
//      var found = true
//
//      var keyToSearch = key.toInt
//      while (isFiltered(keyToSearch)) {
//        found = false
//        keyToSearch += 1
//      }
//
//      found &= trieIter.seek(keyToSearch)
//
//      while (isFiltered(trieIter.key.toInt) && !trieIter.atEnd) {
//        found = false
//        trieIter.next()
//      }
//      found
//    }

  private def isFiltered(value: Int): Boolean = {
    //    println("value", value)
        val hash: Int = hashes(trieIter.getDepth).hash(value)
    ////    println("hash", hash)
    ////    println("coordinate", coordinate(trieIter.getDepth))
        val v = hash != coordinate(trieIter.getDepth)
//      println("Filtered", v)
//        v
    false
  }


}
