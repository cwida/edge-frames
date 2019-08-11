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

  /**
    * Determines the best configuration for Shares given the query structure, variable ordering and number of workers.
    *
    * The script is heavily inspired and influenced by the paper "From Theory to Practice: Efficient Join Query Evaluation in
    * a Parallel Database System" by Shumo et. al., 2015 and their implementation in Myria.
    *
    * We estimate the cost of any given configuration by the numbers of tuples that need to be handled by a specific worker, assuming
    * that this is the 1 / <size of the hypercube plain spanned by it's attributes>. We then choose the configuration with the minimal cost.
    * Out of all configurations with minimal cost we prefer balanced configurations, that is their maximal dimension size is smaller, to
    * avoid skew. Out of all balanced, minimal cost configurations, we prefer the one which have larger dimensions on the ealier variables.
    *
    * This cost estimation was originally developed by Shumo et al for communicational optimality. It could be beneficial to reimplement it
    * for computational optimality. Their are two apparent difference: (1) for compuational optimality it could pay off to pay more
    * attention to the variable ordering, (2) estimate costs on a per variable basis and not a per tuple basis because computational the
    * variables are seen independently from each other and not in the a tuple relationship.
    *
    * @param partitions level of desired parallelism
    * @param query the query to compute a configuration for
    * @param variableOrdering the used variable ordering
    * @param prefix Calculate the best configuration by only taking the first `prefix` variables into account. E.g. a configuration for the
    *               triangle query with prefix 2 might look like (a, b, 1).
    * @return a Hypercube with estimated best configuration
    */
  def getBestConfigurationFor(partitions: Int, query: Query, variableOrdering: Seq[String], prefix: Option[Int] = None): Hypercube = {
    require(query.vertices == variableOrdering.toSet, "Variable ordering should contain all vertices.")

    val start = System.nanoTime()

    val edgesAsIndices = query.edges.map(e => (variableOrdering.indexOf(e._1), variableOrdering.indexOf(e._2)))

    val variableCount = prefix.getOrElse(variableOrdering.size)
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
      while (i < variableCount) {
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

    println(s"finding config took ${(System.nanoTime() - start) / 1e9} seconds. Configuration is: ${bestConfiguration.mkString(", ")}")
    new Hypercube(bestConfiguration)
  }

  private def estimateCosts(query: Seq[(Int, Int)], configuration: Array[Int]): Double = {
    query.map(e => 1.0 / (configuration(e._1) * configuration(e._2))).sum
  }

}