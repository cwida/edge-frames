package leapfrogTriejoin

import scala.collection.mutable

class MaterializingLeapfrogJoin(var iterators: Array[LinearIterator]) extends LeapfrogJoinInterface {
  var shouldMaterialize = true
  private[this] val DELETED_VALUE = -2

  if (iterators.isEmpty) {
    throw new IllegalArgumentException("iterators cannot be empty")
  }

  private[this] var isAtEnd: Boolean = false
  private[this] var keyValue = 0L

  private[this] var initialized = false

  private[this] var firstLevelIterators: Array[LinearIterator] = null
  private[this] var secondLevelIteraors: Array[LinearIterator] = null

  private[this] var materializedValues: Array[Long] = null

  private[this] var position = 0

  private[this] var fallback: LeapfrogJoin = null

  def init(): Unit = {
    if (!initialized) {
      firstLevelIterators = iterators.filter(_.getDepth == 0)
      secondLevelIteraors = iterators.filter(_.getDepth == 1)
//      println("init", secondLevelIteraors.length)
      initialized = true

      if (secondLevelIteraors.length == 0 || !shouldMaterialize) {
        fallback = new LeapfrogJoin(iterators)
      }
    }

    if (secondLevelIteraors.length == 0 || !shouldMaterialize) {
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
    materializedValues = new Array[Long](secondLevelIteraors(0).estimateSize + 1)
    if (secondLevelIteraors.length == 1) {
      materializeSingleIterator(secondLevelIteraors(0))
    } else {
      intersect(secondLevelIteraors(0), secondLevelIteraors(1))

      var i = 2
      while (i < secondLevelIteraors.length) {
        intersect(secondLevelIteraors(i))
        i += 1
      }
    }
    isAtEnd = materializedValues(0) == -1
  }

  private def materializeSingleIterator(iter: LinearIterator): Unit = {
    var i = 0
    while (!iter.atEnd) {
      materializedValues(i) = iter.key
      iter.next() // TODO optimize such that next returns boolean
      i += 1
    }
    materializedValues(i) = -1
  }

  // TODO reused of intersection! see print of mat after first and second intersection

  private def intersect(i1: LinearIterator, i2: LinearIterator): Unit = {
    var valueCounter = 0
    while (!i1.atEnd && !i2.atEnd) {
      //      println("here4")

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
      isAtEnd = firstLevelIterators(i).seek(value)
      if (!isAtEnd) {
        in &= firstLevelIterators(i).key == value
      }
      i += 1
    }
    in
  }

  override def key: Long = keyValue

  override def atEnd: Boolean = isAtEnd
}
