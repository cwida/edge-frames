package leapfrogTriejoin


import scala.collection.Searching._
import _root_.scala.collection.JavaConversions._


// https://stackoverflow.com/questions/4226947/scala-replacement-for-arrays-binarysearch/26023131
class ObjectArrayTools[T <: Int](a: Array[T]) {
  def binarySearch(key: T, min: T, max: T) = {
    java.util.Arrays.binarySearch(a.asInstanceOf[Array[Int]], min, max, key)
  }
}

class UnaryRelationship(values: Array[Int]) extends LinearIterator {
  implicit def anyrefarray_tools[T <: Int](a: Array[T]) = new ObjectArrayTools(a)
  var iteratorPosition: Int = 0

  override def key: Int = values(iteratorPosition)

  override def next(): Unit = {
    iteratorPosition += 1
  }

  override def atEnd: Boolean = iteratorPosition >= values.length

  // TODO yes it is in O(log N). However, it is quite inefficient.
  // TODO use iterator position to limit search space
  override def seek(key: Int): Unit = {
    val i = values.binarySearch(key, iteratorPosition, values.length)
    if (i >= 0) {
      iteratorPosition = i
    } else {
      iteratorPosition = -(i + 1)
    }
  }
}
