package partitioning.shares

import experiments.Query
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.sys.process.Process

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
}

object Hypercube {
  val BEST_CONFIGURATION_SCRIPT = "src/best-configuration.py"

  def getBestConfigurationFor(partitions: Int, query: Query, variableOrdering: Seq[String]): Hypercube = {
    require(query.vertices == variableOrdering.toSet, "Variable ordering should contain all vertices.")

    val configuration = callBestConfigurationScript(partitions, query)
    val dimensionSizes = variableOrdering.map(d => configuration(d)).toArray
    Hypercube(dimensionSizes)
  }

  private def callBestConfigurationScript(partitions: Int, query: Query): Map[String, Int] = {
    val edges: Seq[String] = query.edges.map(e => s"${e._1} ${e._2}").mkString(" ").split(" ")
    val cmd: Seq[String] = Seq("python3", BEST_CONFIGURATION_SCRIPT, partitions.toString) ++ edges
    val output = Process(cmd).lineStream

    parsePythonDict(output.head)
  }

  private def parsePythonDict(dict: String): Map[String, Int] = {
    dict
      .replace("{", "")
      .replace("}", "")
      .replace("'", "")
      .split(",")
      .map(s => {
        val keyValue = s.split(":").map(_.trim)
        (keyValue(0), keyValue(1).toInt)
      })
      .toMap
  }


}