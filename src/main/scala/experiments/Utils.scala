package experiments

import java.lang.management.ManagementFactory
import collection.JavaConverters._

object Utils {

  var lastStats = GarbageCollectionStats(0, 0)

  def avg(i : Traversable[Double]): Double = {
    i.sum / i.size
  }


  def getGarbageCollectionStatsSinceLastCall(): GarbageCollectionStats = {

    val beans = ManagementFactory.getGarbageCollectorMXBeans.asScala

    val totalCount = beans.map(_.getCollectionCount).sum
    val totalTime = beans.map(_.getCollectionTime).sum.toDouble

    val gcStatsSinceLastCall = GarbageCollectionStats(totalCount - lastStats.count, totalTime - lastStats.time)
    lastStats = GarbageCollectionStats(totalCount, totalTime)
    gcStatsSinceLastCall
  }
}

case class GarbageCollectionStats(count: Long, time: Double)
