package leapfrogTriejoin

import scala.collection.mutable

class MaterializingLeapfrogJoin(var iterators: Array[LinearIterator]) extends LeapfrogJoinInterface {
  private[this] val DELETED_VALUE = -2

  if (iterators.isEmpty) {
    throw new IllegalArgumentException("iterators cannot be empty")
  }

  private[this] var isAtEnd: Boolean = false
  private[this] var keyValue = 0L

  private[this] var initialized = false

  private[this] var firstLevelIterators: Array[LinearIterator] = null
  private[this] var secondLevelIterators: Array[LinearIterator] = null

  private[this] var materializedValues: Array[Long] = new Array[Long](200)

  private[this] var position = 0

  private[this] var fallback: LeapfrogJoin = null

  def init(): Unit = {
    if (!initialized) {
      firstLevelIterators = iterators.filter(_.getDepth == 0)
      secondLevelIterators = iterators.filter(_.getDepth == 1)
      initialized = true

      if (secondLevelIterators.length == 0 || !MaterializingLeapfrogJoin.shouldMaterialize) {
        fallback = new LeapfrogJoin(iterators)
      }
    }

    if (secondLevelIterators.length == 0 || !MaterializingLeapfrogJoin.shouldMaterialize) {
      fallback.init()
      isAtEnd = fallback.atEnd
      if (!isAtEnd) {
        keyValue = fallback.key
      }
    } else {
      iteratorAtEndExists()

      keyValue = -1

      if (!isAtEnd) {
        materialize()
        position = -1
        if (!isAtEnd) {
          leapfrogNext()
        }
      }
    }
  }

  private def materialize(): Unit = {
    if (secondLevelIterators.length == 1) {
      if (materializedValues.length < secondLevelIterators(0).estimateSize + 1) {
        materializedValues = new Array[Long](secondLevelIterators(0).estimateSize + 1)
      }
      materializeSingleIterator(secondLevelIterators(0))
    } else {
      moveSmallestIteratorFirst()
      if (materializedValues.length < secondLevelIterators(0).estimateSize + 1) {
        materializedValues = new Array[Long](secondLevelIterators(0).estimateSize + 1)
      }

      intersect(secondLevelIterators(0), secondLevelIterators(1))

      var i = 2
      while (i < secondLevelIterators.length) {
        intersect(secondLevelIterators(i))
        i += 1
      }
    }
    isAtEnd = materializedValues(0) == -1
  }

  @inline
  private def moveSmallestIteratorFirst(): Unit = {
    var i = 0
    var smallestSize = Integer.MAX_VALUE
    var smallestPosition = 0
    while (i < secondLevelIterators.length) {
      val size = secondLevelIterators(i).estimateSize
      if (size < smallestSize) {
        smallestSize = size
        smallestPosition = i
      }
      i += 1
    }

    val temp = secondLevelIterators(0)
    secondLevelIterators(0) = secondLevelIterators(smallestPosition)
    secondLevelIterators(smallestPosition) = temp
  }

  private def materializeSingleIterator(iter: LinearIterator): Unit = {
    var i = 0
    while (!iter.atEnd) {
      materializedValues(i) = iter.key
      iter.next()
      i += 1
    }
    materializedValues(i) = -1
  }

  // TODO reused of intersection! see print of mat after first and second intersection

  private def intersect(i1: LinearIterator, i2: LinearIterator): Unit = {
    var valueCounter = 0
    while (!i1.atEnd && !i2.atEnd) {
      if (i1.key == i2.key) {
        materializedValues(valueCounter) = i1.key
        valueCounter += 1
        i1.next()
        i2.next()
      } else if (i1.key < i2.key) {
        i1.seek(i2.key)
      } else {
        i2.seek(i1.key)
      }
    }
    materializedValues(valueCounter) = -1
  }

  private def intersect(iter: LinearIterator): Unit = { // TODO optimizable?

//    val buffer = mutable.Buffer[Long]()
//    val clone = iter.clone()
//    while (!clone.atEnd) {
//      buffer.append(clone.key)
//      clone.next()
//    }
//    val expected = materializedValues.intersect(buffer)

//    val before = new Array[Long](materializedValues.length)
//    materializedValues.copyToArray(before)

    var i = 0
    var value = materializedValues(i)
    while (value != -1 && !iter.atEnd) {
      if (value != DELETED_VALUE) {
        iter.seek(value)
        if (iter.key != value) {
          materializedValues(i) = DELETED_VALUE
        }
      }
      i += 1
      value = materializedValues(i)
    }

    while (materializedValues(i) != -1) {
      materializedValues(i) = DELETED_VALUE
      i += 1
    }


//    if (!(materializedValues.filter(_ != DELETED_VALUE).takeWhile(_ != -1) sameElements expected)) {
//      println("is", materializedValues.mkString(", "), "but should be", expected.mkString(", ") )
//      println("before", before.mkString(", "))
//      println("buffer", buffer.mkString(", "))
//    }

  }

  @inline
  private def iteratorAtEndExists(): Unit = {
    isAtEnd = false
    var i = 0
    while (i < iterators.length) {
      if (iterators(i).atEnd) {
        isAtEnd = true
      }
      i += 1
    }
  }

  def leapfrogNext(): Unit = {
    if (fallback == null) {
      var found = false

      do {
        position += 1
        keyValue = materializedValues(position)
        if (keyValue >= 0) {
          found |= filterAgainstFirstLevelIterators(keyValue)
        } else if (keyValue == -1) {
          isAtEnd = true
        }
      } while (!found && !isAtEnd)
    } else {
      fallback.leapfrogNext()
      isAtEnd = fallback.atEnd
      if (!isAtEnd) {
        keyValue = fallback.key
      }
    }
  }

  @inline
  private def filterAgainstFirstLevelIterators(value: Long): Boolean = {
    var i = 0
    var in = true
    while (!isAtEnd && i < firstLevelIterators.length) {
      if (!firstLevelIterators(i).seek(value)) {  // TODO can i optimize that for the case that it is nearly always true?
        in &= firstLevelIterators(i).key == value
      } else {
        isAtEnd = true
      }
      i += 1
    }
    in
  }

  override def key: Long = keyValue

  override def atEnd: Boolean = isAtEnd
}

object MaterializingLeapfrogJoin {
  private var shouldMaterialize = true

  def setShouldMaterialize(value: Boolean): Unit ={
    if (value) {
      println("Using materializing Leapfrogjoins")
    } else {
      println("Not using materializing Leapfrogjoins")
    }
    shouldMaterialize = value
  }

}
