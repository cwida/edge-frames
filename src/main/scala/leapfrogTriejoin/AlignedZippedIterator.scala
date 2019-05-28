package leapfrogTriejoin

import org.apache.spark.sql.catalyst.InternalRow

class AlignedZippedIterator(iter1: Iterator[InternalRow], iter2: Iterator[InternalRow]) extends Iterator[Array[Long]] {
  val iter1Buffered = iter1.buffered
  val iter2Buffered = iter2.buffered

  val output = new Array[Long](3)

  override def hasNext: Boolean = {
    iter1Buffered.hasNext || iter2Buffered.hasNext
  }

  override def next(): Array[Long] = { // TODO optimize
    if (iter1Buffered.hasNext && iter2Buffered.hasNext) {
      if (iter1Buffered.head.getLong(0) == iter2Buffered.head.getLong(0)) {
        val iter1Next = iter1Buffered.next()
        val iter2Next = iter2Buffered.next()
        output(0) = iter1Next.getLong(0)
        output(1) = iter1Next.getLong(1)
        output(2) = iter2Next.getLong(1)
      } else if (iter1Buffered.head.getLong(0) < iter2Buffered.head.getLong(0)) {
        val iter1Next = iter1Buffered.next()
        output(0) = iter1Next.getLong(0)
        output(1) = iter1Next.getLong(1)
        output(2) = -1
      } else { // iter2Buffered.head.getLong(0) <
        val iter2Next = iter2Buffered.next()
        output(0) = iter2Next.getLong(0)
        output(1) = -1
        output(2) = iter2Next.getLong(1)
      }
    } else if (iter1Buffered.hasNext) {
      val n = iter1Buffered.next()
      output(0) = n.getLong(0)
      output(1) = n.getLong(1)
      output(2) = -1
    } else { // iter2Buffered.hasNext
      val n = iter2Buffered.next()
      output(0) = n.getLong(0)
      output(1) = -1
      output(2) = n.getLong(1)
    }
    output
  }

}
