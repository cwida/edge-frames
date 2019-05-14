package leapfrogTriejoin

object implicits {
  implicit def leapfrogTriejoin2Iterator(lftj: LeapfrogTriejoin): Iterator[Array[Long]] = {
    new Iterator[Array[Long]] {
      override def hasNext: Boolean = !lftj.atEnd


      override def next(): Array[Long] = {
        lftj.next()
      }
    }
  }
}
