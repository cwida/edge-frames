package leapfrogTriejoin

import java.util.concurrent.ConcurrentLinkedQueue

import sparkIntegration.WCOJConfiguration

class WorkstealingLeapfrogjoin(queue: ConcurrentLinkedQueue[Int],
                               private[this] val localLeapfrog: LeapfrogJoinInterface,
                               private[this] val batchSize: Int) extends LeapfrogJoinInterface {

  private[this] var isAtEnd = queue.isEmpty || localLeapfrog.atEnd

  private[this] var workQueueSize: Int = 0
  private[this] var currentWorkItem: Int = 0

  override def init(): Unit = {
    localLeapfrog.init()
    leapfrogNext()
  }

  override def leapfrogNext(): Unit = {
    do {
      if (workQueueSize == 0) {
        currentWorkItem = queue.poll()
        workQueueSize = batchSize - 1
      } else {
        currentWorkItem += 1
        workQueueSize -= 1
      }
      if (localLeapfrog.key < currentWorkItem) {
        localLeapfrog.leapfrogSeek(currentWorkItem)
      }
    } while (!(queue.isEmpty && workQueueSize == 0) && !localLeapfrog.atEnd && localLeapfrog.key != currentWorkItem)
    isAtEnd = (queue.isEmpty && workQueueSize == 0) || localLeapfrog.atEnd
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
