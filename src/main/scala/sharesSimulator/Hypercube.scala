package sharesSimulator

import experiments.Query
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.sys.process.Process

class Hypercube(workers: Int, query: Query, conf: Option[Seq[Int]] = None) extends Serializable {
  val BEST_CONFIGURATION_SCRIPT = "src/best-configuration.py"

  private val vertices: Seq[String] = query.vertices.toSeq.sorted
  private val dimensions = vertices.size

  private val configuration = conf.getOrElse(callBestConfigurationScript)

  private val hashes = vertices.zipWithIndex.map {
    case (v, i) => (v, Hash(i, configuration(i)))
  }.toMap

  private val verticeToDimension = vertices.zipWithIndex.toMap

  private val WILD_VALUE = -1

  def getWorkers(tuple: Row, cols: (String, String)): Seq[Int] = {
    getCoordinates(getWildCoordinate(tuple, cols)).map(coordinate2Worker)
  }

  private def getWildCoordinate(tuple: Row, cols: (String, String)): Seq[Int] = {
    val coordinate = Array.fill(dimensions)(WILD_VALUE)

    coordinate(verticeToDimension(cols._1)) = hashes(cols._1).hash(tuple.getLong(0).toInt)
    coordinate(verticeToDimension(cols._2)) = hashes(cols._2).hash(tuple.getLong(1).toInt)
    coordinate
  }

  private def getCoordinates(wildCoordinate: Seq[Int]): Seq[Seq[Int]] = {
    val possibleValuesPerDimension = wildCoordinate.zipWithIndex.map {
      case (WILD_VALUE, i) => 0 until configuration(i)
      case (v, i) => Seq(v)
    }
    Utils.cartesian(possibleValuesPerDimension.map(_.toList).toList)
  }

  // Public for testing
  def coordinate2Worker(c: Seq[Int]): Int = {
    var multiplicator = 1
    var worker = 0
    for ((v, i)  <- c.zipWithIndex) {
      worker += v * multiplicator
      multiplicator = configuration(i) * multiplicator
    }
    worker
  }

  private def callBestConfigurationScript: Seq[Int] = {
    val edges: Seq[String] = query.edges.map(e => s"${e._1} ${e._2}").mkString(" ").split(" ")
    val cmd: Seq[String] = Seq("python3", BEST_CONFIGURATION_SCRIPT, workers.toString) ++ edges
    val output = Process(cmd).lineStream

    val verticesToDimesionSize = parsePythonDict(output.head)
    vertices.map(v => {
      verticesToDimesionSize(v)
    })
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


  def calculateWorkers(ds: DataFrame): RDD[(Int, (Long, Long))] = {
    val edges = query.edges.sorted.toArray  // Sort to get deterministic results (the order influences the hash used per edge)
    ds.rdd.flatMap(t =>
      edges.flatMap(cols => getWorkers(t, cols)).map(w => (w, (t.getLong(0), t.getLong(1))))
      )
      .distinct() // Optimization, each tuples needs to be sent only once to each worker even
    // when assigned by multiple edge relationships.
  }

}
