package leapfrogTriejoin

import partitioning.{AllTuples, Partitioning, Shares}
import partitioning.shares.Hash

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Optimized Leapfrog join for small iterators and intersections.
  *
  * If the resulting intersections are small, it is faster to materialize them into an array all at once
  * and return results from this buffer on every next call, due to better locality.
  *
  * This optimized version materializes the intersection of 2nd level iterators (usual very small in graphs)
  * and intersects the intersection of 2nd level iterators with the 1st level iterators in the next call.
  * This is a conscious design decission because the 1st level iterators need to be moved to the right
  * position on every next call anyhow - hence they need to be touched.
  *
  * We use iterative 2-way in-tandem intersection building to intersect the 2nd level itertors.
  * We start this process with the smallest interator to limit the intesection as much as possible from the
  * beginning.
  *
  * @param iterators    the TrieIterators to operate on. Leapfrogjoin works only on the linear component
  * @param variable     the variable the join acts upon as index in the global variable ordering
  * @param partition    the partition to work upon
  * @param partitioning the paritioning used for parallelization
  */
class MaterializingLeapfrogJoin(var iterators: Array[TrieIterator],
                                val variable: Int,
                                val partition: Int,
                                val partitioning: Partitioning) extends LeapfrogJoinInterface {

  def this(iterators: Array[TrieIterator]) = {
    this(iterators, 0, 0, AllTuples())
  }

  private[this] val DELETED_VALUE = -2

  require(iterators.nonEmpty, "iterators cannot be empty")

  private[this] var isAtEnd: Boolean = false
  private[this] var keyValue = 0L

  /**
    * true when this join has been initialized once before.
    */
  private[this] var initialized = false

  private[this] var firstLevelIterators: Array[TrieIterator] = _
  private[this] var secondLevelIterators: Array[TrieIterator] = _

  /**
    * Position in the materializedValues array used to iterator over it.
    */
  private[this] var position = 0
  private[this] var materializedValues: Array[Long] = new Array[Long](200)

  /**
    * fallback to a normal/none-materialized LeapfrogJoin in case all iterators are at the first level or materializing should not be
    * used by configuration.
    */
  private[this] var fallback: LeapfrogJoin = _

  /* Shares paritioning */

  /**
   * Coordinate component along the dimension of @field{variable}, -1 for all other partitionings.
   * Used to split the code path for Shares partitioning.
   */
  private[this] var coordinate: Int = -1
  private[this] var hash: Hash = _

  partitioning match {
    case Shares(hypercube) => {
      coordinate = hypercube.getCoordinate(partition)(variable)
      hash = hypercube.getHash(variable)
    }
    case _ => { /* NOP */}
  }

  def init(): Unit = {
    if (!initialized) {
      firstInitialization()
    }

    if (fallback != null) {
      fallback.init()

      while (coordinate != -1 && !fallback.atEnd && hash.hash(fallback.key.toInt) != coordinate) {
        fallback.leapfrogNext()
      }

      isAtEnd = fallback.atEnd
      if (!isAtEnd) {  // TODO unnessesary if?
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

  private def firstInitialization(): Unit = {
    firstLevelIterators = iterators.filter(_.getDepth == 0)
    secondLevelIterators = iterators.filter(_.getDepth == 1)
    initialized = true

    if (secondLevelIterators.length == 0 || !MaterializingLeapfrogJoin.shouldMaterialize) {
      fallback = new LeapfrogJoin(iterators.map(_.asInstanceOf[LinearIterator]))
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
    do {
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
        if (!isAtEnd) {  // TODO unnessesary if?
          keyValue = fallback.key
        }
      }
    } while (coordinate != -1 && !isAtEnd && hash.hash(keyValue.toInt) != coordinate) // TODO optimizable by combining loops?
  }

  @inline
  private def filterAgainstFirstLevelIterators(value: Long): Boolean = {
    var i = 0
    var in = true
    while (!isAtEnd && i < firstLevelIterators.length) {
      if (!firstLevelIterators(i).seek(value)) { // TODO can i optimize that for the case that it is nearly always true?
        in &= firstLevelIterators(i).key == value
      } else {
        isAtEnd = true
      }
      i += 1
    }
    in
  }

  override def key: Long = {
    keyValue
  }

  override def atEnd: Boolean = {
    isAtEnd
  }
}

object MaterializingLeapfrogJoin {
  private var shouldMaterialize = true

  def setShouldMaterialize(value: Boolean): Unit = {
    if (value) {
      println("Leapfrogjoins ARE materialized")
    } else {
      println("Leapfrogjoins are NOT materialized")
    }
    shouldMaterialize = value
  }

}
