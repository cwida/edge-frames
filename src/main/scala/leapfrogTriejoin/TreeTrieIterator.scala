package leapfrogTriejoin

import org.slf4j.LoggerFactory

import scala.collection.immutable.TreeMap
import Ordering.Implicits._

// Only as reference implemenation and for testing. Otherwise, unused.
class TreeTrieIterator(val values: Array[(Long, Long)]) extends TrieIterator {
  private val logger = LoggerFactory.getLogger(classOf[TreeTrieIterator])

  val HIGHEST_LEVEL = -1
  var map = new TreeMap[Vector[Long], Int]()

  for ((t, i) <- values.zipWithIndex) {
    map = map.updated(Vector(t._1, t._2), i)
  }

  var depth = HIGHEST_LEVEL
  var isAtEnd = map.isEmpty
  var isAtTotalEnd = map.isEmpty
  val maxDepth = 1
  var triePath = Vector.fill(maxDepth + 1){-1L}
  var mapIterator = map.keysIterator

  def up(): Unit = {
    if(depth == HIGHEST_LEVEL) {
      throw new IllegalStateException("Cannot go up in TrieIterator at root level.")
    }
    triePath = triePath.updated(depth, -1L)
    depth -= 1
    isAtEnd = false
  }

  def open(): Unit = {
    if (depth == maxDepth) {
      throw new IllegalStateException("Cannot go down in TrieIterator at lowest level.")
    } else{
      depth += 1
      mapIterator = map.keysIteratorFrom(triePath)
      triePath = triePath.updated(depth, mapIterator.next()(depth))
      isAtEnd = false
    }
  }

  override def key: Long = triePath(depth)

  override def next(): Unit = {
    seek(triePath(depth) + 1)
  }

  override def atEnd: Boolean = isAtEnd

  override def seek(key: Long): Boolean = {
    if (atEnd) {
      throw new IllegalStateException("Cannot move to next at end of branch.")
    }

    val temp = mapIterator
    var possiblyNewNextVector = triePath.updated(depth, key)
    mapIterator = map.keysIteratorFrom(possiblyNewNextVector)
    if (mapIterator.isEmpty) {
      isAtEnd = true
      isAtTotalEnd = true
    } else {
      possiblyNewNextVector = mapIterator.next()
      if (0 < depth && possiblyNewNextVector(depth - 1) != triePath(depth - 1)) {
        isAtEnd = true
        mapIterator = temp
      } else {
        triePath = triePath.updated(depth, possiblyNewNextVector(depth))
      }
    }
    atEnd
  }

  @inline
  override def translate(keys: Array[Long]): Array[Long] = {
    keys
  }

  override def estimateSize: Int = {
    -1
  }

  override def min: Int = {
    ???
  }

  override def max: Int = ???

  override def clone(): LinearIterator = ???

  override def getDepth: Int = {
    ???
  }
}
