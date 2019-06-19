package sharesSimulator

import experiments.Query
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.sys.process.Process

class Hypercube(workers: Int, query: Query) {
  val BEST_CONFIGURATION_SCRIPT = "src/best-configuration.py"

  private val vertices = query.vertices.toSeq
  private val dimensions = vertices.size

  private val configuration = callBestConfigurationSkript

  private val hashes = vertices.zipWithIndex.map {
    case (v, i) => (v, new Hash(i, configuration(i)))
  }.toMap

  private val verticeToDimension = vertices.zipWithIndex.toMap

  private val WILD_VALUE = -1

  def getWorkers(tuple: Row, cols: (String, String)): Seq[Int] = {
    getCoordinates(getWildCoordinate(tuple, cols)).map(coordinate2Worker)
  }

  private def getWildCoordinate(tuple: Row, cols: (String, String)): Seq[Int] = {
    val coordinate = Array.fill(dimensions)(WILD_VALUE)
    coordinate(verticeToDimension(cols._1)) = hashes(cols._1).hash(tuple.getInt(0))
    coordinate(verticeToDimension(cols._2)) = hashes(cols._2).hash(tuple.getInt(1))
    coordinate
  }

  private def getCoordinates(wildCoordinate: Seq[Int]): Seq[Seq[Int]] = {
    val possibleValuesPerDimension = wildCoordinate.zipWithIndex.map {
      case (WILD_VALUE, i) => 0 until configuration(i)
      case (v, i) => Seq(v)
    }
    cartesian(possibleValuesPerDimension)
  }

  private def coordinate2Worker(c: Seq[Int]): Int = {
    var multiplicator = 1
    var worker = 0
    for ((i, v)  <- c.zipWithIndex) {
      worker += v * multiplicator
      multiplicator = configuration(i) * multiplicator
    }
    worker
  }

  private def cartesian(seq: Seq[Seq[Int]]): Seq[Seq[Int]] = {
    ???
  }

  private def callBestConfigurationSkript: Seq[Int] = {
    val edges: Seq[String] = query.edges.map(e => s"${e._1} ${e._2}").mkString(" ").split(" ")
    val cmd: Seq[String] = Seq("python3", BEST_CONFIGURATION_SCRIPT, workers.toString) ++ edges
    val output = Process(cmd).lineStream
    output.head.split(",").map(_.toInt)
  }


  def calculateWorkers(ds: DataFrame): RDD[(Int, Row)] = {
    // TODO would I need to copy the row?
    ds.rdd.flatMap(t =>
      query.edges.flatMap(cols => getWorkers(t, cols)).map(w => (w, t))
      )
  }

}
