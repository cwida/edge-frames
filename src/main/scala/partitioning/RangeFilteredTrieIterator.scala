package partitioning

import leapfrogTriejoin.TrieIterator

/**
  * A TrieIterator that filters an underlying TrieIterator such that only values of certain ranges are returned.
  *
  * @param ranges1stLevel the ranges to filter the first level of the trieIterator, odd indices determine the lower, inclusive bound of a
  *                       range, even indices give the upper, exclusive bound of a range. Only keys within a range are returned, all
  *                       others are filetered. The ranges cannot overlap. The ranges need to be sorted in ascending order.
  * @param ranges2ndLevel see ranges1stLevel
  * @param trieIterator
  */
class RangeFilteredTrieIterator(
                                       val partition: Int,
                                       val ranges1stLevel: Array[Int],
                                       val ranges2ndLevel: Array[Int],
                                       val trieIterator: TrieIterator) extends TrieIterator {
  private[this] var firstLevelRangePosition = 0
  private[this] var secondLevelRangePosition = 0

  private[this] var isAtEnd = false

  override def open(): Unit = {
    trieIterator.open()
    isAtEnd = trieIterator.atEnd

    if (!isAtEnd) {
      if (trieIterator.getDepth == 0) {
        firstLevelRangePosition = 0
        if (trieIterator.key < ranges1stLevel(firstLevelRangePosition)) {
          trieIterator.seek(ranges1stLevel(firstLevelRangePosition))
          isAtEnd = trieIterator.atEnd
        }
        updateRange()
      } else if (trieIterator.getDepth == 1) {
        secondLevelRangePosition = 0
        if (trieIterator.key < ranges2ndLevel(secondLevelRangePosition)) {
          trieIterator.seek(ranges2ndLevel(secondLevelRangePosition))
          isAtEnd = trieIterator.atEnd
        }
        updateRange()
      }
    }
  }

  override def up(): Unit = {
    trieIterator.up()
    isAtEnd = trieIterator.atEnd
  }

  override def key: Long = {
    trieIterator.key
  }

  override def next(): Unit = {
    trieIterator.next()
    isAtEnd = trieIterator.atEnd
    updateRange()
  }

  private def updateRange(): Unit = {
    // TODO faster with array instead of variables per depth?
    if (!isAtEnd) {
      if (trieIterator.getDepth == 0 && ranges1stLevel(firstLevelRangePosition + 1) <= trieIterator.key) {
        while (firstLevelRangePosition + 1 < ranges1stLevel.length && ranges1stLevel(firstLevelRangePosition + 1) <= trieIterator.key) {
          firstLevelRangePosition += 2
        }

        if (ranges1stLevel.length <= firstLevelRangePosition) {
          isAtEnd = true
        } else {
          if (trieIterator.key < ranges1stLevel(firstLevelRangePosition)) {
            seek(ranges1stLevel(firstLevelRangePosition))
          }
        }
      } else if (trieIterator.getDepth == 1 && ranges2ndLevel(secondLevelRangePosition + 1) <= trieIterator.key) {
        while (secondLevelRangePosition + 1 < ranges2ndLevel.length && ranges2ndLevel(secondLevelRangePosition + 1) <= trieIterator.key) {
          secondLevelRangePosition += 2
        }

        if (ranges2ndLevel.length <= secondLevelRangePosition) {
          isAtEnd = true
        } else {
          if (trieIterator.key < ranges2ndLevel(secondLevelRangePosition)) {
            seek(ranges2ndLevel(secondLevelRangePosition))
          }
        }
      }
    }
  }

  override def seek(key: Long): Boolean = {
    isAtEnd = trieIterator.seek(key)
    updateRange()
    isAtEnd
  }

  override def atEnd: Boolean = {
    isAtEnd
  }

  override def translate(keys: Array[Long]): Array[Long] = {
    trieIterator.translate(keys)
  }

  override def getDepth: Int = {
    trieIterator.getDepth
  }

  // TODO good estimate size implementation
  override def estimateSize: Int = {
    trieIterator.estimateSize
  }
}
