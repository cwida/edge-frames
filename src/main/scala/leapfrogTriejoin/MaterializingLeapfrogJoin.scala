package leapfrogTriejoin

import scala.collection.mutable

class MaterializingLeapfrogJoin(var iterators: Array[LinearIterator]) extends LeapfrogJoinInterface {
  if (iterators.isEmpty) {
    throw new IllegalArgumentException("iterators cannot be empty")
  }

  var atEnd: Boolean = false
  var key = 0L

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
      println("init", secondLevelIteraors.length)
      initialized = true

      if (secondLevelIteraors.length == 0) {
        fallback = new LeapfrogJoin(iterators)
      }
    }

    if (secondLevelIteraors.length == 0) {
      fallback.init()
      atEnd = fallback.atEnd
      if (!atEnd) {
        key = fallback.key
      }
    } else {
      iteratorAtEndExists()

      key = -1

      if (!atEnd) {
        materialize()
        position = -1
        if (!atEnd) {
          leapfrogNext()
        }
      }
    }
  }

  private def materialize(): Unit = {
    materializedValues = new Array[Long](secondLevelIteraors.head.estimateSize + 1)
    if (secondLevelIteraors.length == 1) {
      materializeSingleIterator(secondLevelIteraors.head)
    } else {
      intersect(secondLevelIteraors(0), secondLevelIteraors(1))

      var i = 2
      while (i < secondLevelIteraors.length) {
        intersect(secondLevelIteraors(i))
        i += 1
      }
    }
    atEnd = materializedValues.head == -1


    val withoutEnd = materializedValues.takeWhile(_ != -1).toArray
    assert(Set(withoutEnd).size == withoutEnd.size, s"mat is not a set $withoutEnd")
    //    println(materializedValues.mkString(", "))
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
//    if (secondLevelIteraors.length == 3) {
//      println("mat 1", materializedValues.mkString(", "))
//    }
  }

  private def intersect(iter: LinearIterator): Unit = {  // TODO optimizable?
    var i = 0
//    while (materializedValues(i) != -1 && !iter.seek(materializedValues(i))) {
//      //      println("here3")
//
//      if (iter.key != materializedValues(i)) {
//        var j = i // TODO inneficient???
//        do {
//          materializedValues(j) = materializedValues(j + 1)
//          j += 1
//        } while (materializedValues(j) != -1)
//      } else {
//        i += 1
//      }
//    }

    // TODO inneficient

    val buffer = mutable.Buffer[Long]()
    while (!iter.atEnd) {
      buffer.append(iter.key)
      iter.next()
    }
    materializedValues = materializedValues.intersect(buffer) ++ Seq(-1L)

//    println("mat 2", materializedValues.mkString(", "))

  }

  @inline
  private def iteratorAtEndExists(): Unit = {
    atEnd = false
    var i = 0
    while (i < iterators.length) {
      if (iterators(i).atEnd) {
        atEnd = true
      }
      i += 1
    }
  }

  def leapfrogNext(): Unit = {
    if (fallback == null) {
      var found = false
      do  {
//        println("here")
        position += 1  // TODO moves position even though value was found
        found |= filterAgainstFirstLevelIterators(materializedValues(position))
      } while (!found && !atEnd)

      key = materializedValues(position)
    } else {
      fallback.leapfrogNext()
      atEnd = fallback.atEnd
      if (!atEnd) {
        key = fallback.key
      }
    }
  }

  @inline
  private def filterAgainstFirstLevelIterators(value: Long): Boolean = {
    if (value != -1) {  // TODO get rid of this if
      var i = 0
      var in = true
      while (!atEnd && i < firstLevelIterators.length) {
//        println("here1")

        atEnd = firstLevelIterators(i).seek(value)
        if (!atEnd) {
          in &= firstLevelIterators(i).key == value
        }
        i += 1
      }
      in
    } else {
      atEnd = true
      false
    }
  }

}
