package experiments.metrics

import java.util

import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.{Accumulator, SparkContext}
import partitioning.Partitioning

import scala.collection.JavaConverters._
import scala.collection.mutable

object Metrics {
  val masterTimers: mutable.Map[String, Long] = new mutable.HashMap[String, Long]()

  var lastUsedInitializedPartitioning: Option[Partitioning] = None

  private val timers: mutable.Map[String, CollectionAccumulator[(Int, Long)]] =
    new mutable.HashMap[String, CollectionAccumulator[(Int, Long)]]()


  def getTimer(sc: SparkContext, name: String): CollectionAccumulator[(Int, Long)] = {
    val a = sc.collectionAccumulator[(Int, Long)](name)
    timers.put(name, a)
    a
  }

  def getTimes(name: String): List[(Int, Long)] = {
    timers.get(name) match {
      case Some(x) => x.value.asScala.toList
      case None => Nil
    }
  }


}
