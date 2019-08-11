package partitioning.shares

import experiments.Query
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.sys.process.Process
import collection.{Iterable, mutable}
import math.Ordering._

case class Hypercube(dimensionSizes: Array[Int]) {

  def getCoordinate(partition: Int): Array[Int] = {
    val coordinate = Array.fill(dimensionSizes.size)(-1)
    var multiplicator = dimensionSizes.product
    var rest = partition

    for (i <- dimensionSizes.indices.reverse) {
      multiplicator /= dimensionSizes(i)
      coordinate(i) = rest / multiplicator
      rest -= coordinate(i) * multiplicator
    }
    require(rest == 0)
    coordinate
  }

  def getHash(dimension: Int): Hash = {
    new Hash(dimension, dimensionSizes(dimension))
  }

  override def toString: String = {
    s"Hypercube(Dimensions(${dimensionSizes.mkString(", ")}))"
  }
}

object Hypercube {
  val BEST_CONFIGURATION_SCRIPT = "src/best-computational-configuration.py"

  def getBestConfigurationFor(partitions: Int, query: Query, variableOrdering: Seq[String]): Hypercube = {
    require(query.vertices == variableOrdering.toSet, "Variable ordering should contain all vertices.")

    val start = System.nanoTime()

    val edgesAsIndices = query.edges.map(e => (variableOrdering.indexOf(e._1), variableOrdering.indexOf(e._2)))

    var bestConfiguration = Array.fill(variableOrdering.size)(1)
    var minimalCost = 1.0 * edgesAsIndices.length

    val visited = mutable.Set[Array[Int]]()
    val toVisit = mutable.Queue[Array[Int]]()
    toVisit.enqueue(bestConfiguration)

    while (toVisit.nonEmpty) {
      val c = toVisit.dequeue()
      val estimatedCosts = estimateCosts(edgesAsIndices, c)

      if (estimatedCosts < minimalCost) {
        minimalCost = estimatedCosts
        bestConfiguration = c
      } else if (estimatedCosts == minimalCost) {
        if (c.max < bestConfiguration.max) {  // Try to choose a configuration of even dimension sizes to avoid skew
          bestConfiguration = c
        } else if (c.max == bestConfiguration.max)  {
          // If the configuration is even and has the same cost, try to have the larger values on the first variables
          bestConfiguration = Seq(c.toIterable, bestConfiguration.toIterable).sorted.reverse.head.toArray
        }
      }

      var i = 0
      while (i < c.length) {
        val newConfig = c.clone()
        newConfig(i) = c(i) + 1
        if (newConfig.product <= partitions
          && !visited.contains(newConfig)) {
          toVisit.enqueue(newConfig)
        }
        i += 1
      }
      visited.add(c)
    }

    println("finding config", (System.nanoTime() - start) / 1e9)
    new Hypercube(bestConfiguration)
  }

  private def estimateCosts(query: Seq[(Int, Int)], configuration: Array[Int]): Double = {
    query.map(e => 1.0 / (configuration(e._1) * configuration(e._2))).sum
  }

}