package leapfrogTriejoin

import java.util.concurrent.ConcurrentLinkedQueue

import sparkIntegration.WCOJConfiguration

import scala.collection.mutable

class WorkstealingLeapfrogjoin(queue: ConcurrentLinkedQueue[Int],
                               private[this] val localLeapfrog: LeapfrogJoinInterface,
                               private[this] val batchSize: Int) extends LeapfrogJoinInterface {

  private[this] var isAtEnd = queue.isEmpty || localLeapfrog.atEnd

  private[this] var workQueueSize: Int = 0
  private[this] var currentWorkItem: Int = 0

  private[this] var tasks: Long = 0

  override def init(): Unit = {
    localLeapfrog.init()
    leapfrogNext()
  }

  override def leapfrogNext(): Unit = {
    // Before because queue.isEmpty = true and workQueueSize = 0 means that one last value is available. Hence, we should only
    // set atEnd after returning this value.
    isAtEnd = (queue.isEmpty && workQueueSize == 0) || localLeapfrog.atEnd
    if (!isAtEnd) {
      do {
        if (workQueueSize == 0) {
          currentWorkItem = queue.poll()
          if (queue.isEmpty && currentWorkItem == 0) {
            workQueueSize = 0
          } else {
            tasks += 1
            workQueueSize = batchSize - 1
          }
        } else {
          currentWorkItem += 1
          workQueueSize -= 1
        }
        if (localLeapfrog.key < currentWorkItem) {
          localLeapfrog.leapfrogSeek(currentWorkItem)
        }
      } while (
        !(queue.isEmpty && workQueueSize == 0)
          && !localLeapfrog.atEnd
          && !(localLeapfrog.key <= currentWorkItem + workQueueSize))
      workQueueSize -= localLeapfrog.key.toInt - currentWorkItem
      // Case: queue.isEmpty == true, workQueueSize == 0 and (localLeapfrog.key <= currentWorkItem +workQueueSize) == false.
      // In words, the loop should fetch the next batch but there is no next batch. Therefore, it ends even though it's on a value that
      // does not belong to its batch.
      isAtEnd = workQueueSize < 0
      currentWorkItem = localLeapfrog.key.toInt
    }

    // In the end because the loop above could have moved the localLeapfrog to its end.
    isAtEnd = isAtEnd || localLeapfrog.atEnd
  }

  override def key: Long = {
    localLeapfrog.key
  }

  override def atEnd: Boolean = {
    isAtEnd
  }

  override def leapfrogSeek(key: Long): Unit = {
    ???
  }

  def getTasks: Long = {
    tasks
  }
}
