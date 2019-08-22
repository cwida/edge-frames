package leapfrogTriejoin

import java.util.concurrent.ConcurrentLinkedQueue

class WorkstealingLeapfrogjoin(queue: ConcurrentLinkedQueue[Int],
                               val localLeapfrog: LeapfrogJoinInterface) extends LeapfrogJoinInterface {

  private[this] var isAtEnd = queue.isEmpty || localLeapfrog.atEnd

  override def init(): Unit = {
    localLeapfrog.init()
    leapfrogNext()
  }

  override def leapfrogNext(): Unit = {
    var nextPossibleValue = -1
    do {
      nextPossibleValue = queue.poll()
      localLeapfrog.leapfrogSeek(nextPossibleValue)
    } while (!queue.isEmpty && !localLeapfrog.atEnd && localLeapfrog.key != nextPossibleValue)
    isAtEnd = queue.isEmpty || localLeapfrog.atEnd
  }

  override def key: Long = {
    localLeapfrog.key
  }

  override def atEnd: Boolean = {
    isAtEnd
  }

  override def leapfrogSeek(key: Long): Unit = {
    ???  // TODO maybe implement?
  }
}
