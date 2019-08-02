package partitioning.shares

import experiments.Query
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.sys.process.Process

case class Hypercube(dimensionSizes: Array[Int]) {

  def getCoordinate(partition: Int): Array[Int] = {
    val coordinate = Array.fill(dimensionSizes.size)(0)
    var multiplicator = dimensionSizes.init.product
    var rest = partition

    val range = (1 until dimensionSizes.size).reverse
    for (i <- range) {
      coordinate(i) = (rest / multiplicator)
      rest -= coordinate(i) * multiplicator
      multiplicator /= dimensionSizes(i - 1)
    }
    coordinate(0) = rest
    coordinate
  }
}

object Hypercube {
  val BEST_CONFIGURATION_SCRIPT = "src/best-configuration.py"

  def getBestConfigurationFor(partitions: Int, query: Query): Hypercube = {
    val configuration = callBestConfigurationScript(partitions, query)
    val dimensionsSorted = configuration.keys.toSeq.sorted
    val dimensionSizes = dimensionsSorted.map(d => configuration(d)).toArray
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