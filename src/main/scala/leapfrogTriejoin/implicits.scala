package leapfrogTriejoin

object implicits {
  implicit def leapfrogTriejoin2Iterator(lftj: LeapfrogTriejoin): Iterator[Array[Int]] = {
    new Iterator[Array[Int]] {
      override def hasNext: Boolean = !lftj.atEnd


      override def next(): Array[Int] = {
        lftj.next()
      }
    }
  }
}
