package experiments

import java.io.{BufferedWriter, File, FileWriter}

import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{AccumulableInfo, SparkListener, SparkListenerEvent, SparkListenerExecutorMetricsUpdate, SparkListenerJobEnd, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.ui.{SQLAppStatusListener, SQLAppStatusStore, SparkListenerSQLExecutionEnd}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.kvstore.InMemoryStore
import scopt.OParser
import sparkIntegration.{WCOJ2WCOJExec, WCOJExec}

import scala.collection.mutable.ListBuffer

object Readers {
  implicit def algorithmRead: scopt.Read[Algorithm] = {
    scopt.Read.reads({
      case "WCOJ" => {
        WCOJ
      }
      case "bin" => {
        BinaryJoins
      }
      case _ => {
        throw new IllegalArgumentException("Algorithm can be only `WCOJ` or `bin`")
      }
    })
  }

  implicit def datasetTypeRead: scopt.Read[DatasetType] = {
    scopt.Read.reads({
      case "ama" => {
        AmazonCoPurchase
      }
      case "snb" => {
        SNB
      }
      case "liv" => {
        LiveJournal2010
      }
      case _ => {
        throw new IllegalArgumentException("Dataset type can be only `ama` or `snb`")
      }
    })
  }

  implicit def queryRead: scopt.Read[Query] = {
    scopt.Read.reads(s => {
      val queryTypes = Seq("cycle", "clique", "path", "diamond", "house")

      queryTypes.find(t => s.startsWith(t)) match {
        case Some(t) => {
          val parameter = s.replace(t, "")
          t match {
            case "cycle" => {
              val size = parameter.toInt
              Cycle(size)
            }
            case "clique" => {
              val size = parameter.toInt
              println(s"clique of size: $size")
              Clique(size)
            }
            case "path" => {
              val parts = parameter.split('|')
              PathQuery(parts(0).toInt, parts(1).toDouble)
            }
            case "diamond" => {
              DiamondQuery()
            }
            case "house" => {
              HouseQuery()
            }
            case _ => {
              println(s)
              throw new IllegalArgumentException(s"Unknown query: $s")
            }
          }
        }
        case None => {
          println(s)
          throw new IllegalArgumentException(s"Unknown query: $s")
        }
      }
    })
  }
}

sealed trait Algorithm {
}

case object WCOJ extends Algorithm {
}

case object BinaryJoins extends Algorithm {
}

sealed trait DatasetType {
}

case object AmazonCoPurchase extends DatasetType {
}

case object SNB extends DatasetType {
}

case object LiveJournal2010 extends DatasetType {
}

sealed trait Query

case class Clique(size: Int) extends Query

case class Cycle(size: Int) extends Query

case class PathQuery(size: Int, selectivity: Double) extends Query

case class DiamondQuery() extends Query

case class HouseQuery() extends Query


case class ExperimentConfig(
                             algorithms: Seq[Algorithm] = Seq(WCOJ, BinaryJoins),
                             datasetType: DatasetType = AmazonCoPurchase,
                             datasetFilePath: String = ".",
                             queries: Seq[Query] = Seq.empty,
                             outputPath: File = new File("."),
                             reps: Int = 1,
                             limitDataset: Int = -1
                           )


sealed trait QueryResult {
  def algorithm: Algorithm

  def query: Query

  def count: Long

  def time: Double
}

case class BinaryQueryResult(query: Query, count: Long, time: Double) extends QueryResult {
  override def algorithm: Algorithm = {
    BinaryJoins
  }
}

case class WCOJQueryResult(query: Query, count: Long, time: Double, wcojTime: Double, copyTime: Double, materializationTime: Double) extends QueryResult {
  override def algorithm: Algorithm = {
    WCOJ
  }
}


object ExperimentRunner extends App {

  val config: ExperimentConfig = parseArgs().orElse(throw new IllegalArgumentException("Couldn't parse args")).get

  println("Setting up Spark")
  val sp = setupSpark()

  val ds = loadDataset()

  val wcojTimes = ListBuffer[Double]()
  val copyTimes = ListBuffer[Double]()
  val materializationTimes = ListBuffer[Double]()
  setupMetricListener(wcojTimes, materializationTimes, copyTimes)

  var csvWriter: CSVWriter = null

  setupResultReporting()

  runQueries()

  scala.io.StdIn.readLine("Stop?")

  sp.stop()

  private def parseArgs(): Option[ExperimentConfig] = {
    import Readers._

    val builder = OParser.builder[ExperimentConfig]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("experiment-runner"),
        head("experiment-runner", "0.1"),
        opt[Seq[Algorithm]]('a', "algorithms")
          .required()
          .action((x, c) => c.copy(algorithms = x))
          .text("The algorithm to run experiments with, `bin` or `WCOJ`"),
        opt[DatasetType]('d', "dataset-type")
          .required()
          .action((x, c) => c.copy(datasetType = x)),
        opt[File]('o', "out")
          .valueName("<measurements-output-folder>")
          .required()
          .action((x, c) => c.copy(outputPath = x)),
        opt[String]('i', "dataset-path")
          .required()
          .action((x, c) => c.copy(datasetFilePath = x)),
        opt[Seq[Query]]('q', "queries")
          .valueName("<query1>,<query2>...")
          .required()
          .action((x, c) => c.copy(queries = x)),
        opt[Int]('l', "limit")
          .optional
          .action((x, c) => c.copy(limitDataset = x)),
        opt[Int]('r', "reps")
          .optional()
          .action((x, c) => c.copy(reps = x))
      )
    }
    OParser.parse(parser1, args, ExperimentConfig())
  }

  private def setupSpark(): SparkSession = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("Spark test")
      .set("spark.executor.memory", "40g")
      .set("spark.driver.memory", "20g")
      .set("spark.sql.autoBroadcastJoinThreshold", "104857600") // High threshold
    //          .set("spark.sql.autoBroadcastJoinThreshold", "-1")  // No broadcast
//          .set("spark.sql.codegen.wholeStage", "false")
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    spark.experimental.extraStrategies = (Seq(WCOJ2WCOJExec) ++ spark.experimental.extraStrategies)
    spark
  }

  private def loadDataset(): DataFrame = {
    val dt = config.datasetType

    println(s"Loading ${dt} dataset from ${config.datasetFilePath}")
    var d = config.datasetType match {
      case AmazonCoPurchase => {
        Datasets.loadAmazonDataset(config.datasetFilePath, sp)
      }
      case SNB => {
        Datasets.loadSNBDataset(sp, config.datasetFilePath)
      }
      case LiveJournal2010 => {
        Datasets.loadLiveJournalDataset(sp, config.datasetFilePath)
      }
    }
    if (config.limitDataset != -1) {
      d = d.limit(config.limitDataset)
    }
    d = d.cache()

    val count = d.count() // Trigger dataset caching
    println(s"Running on $count rows")
    d
  }

  private def setupResultReporting(): Unit = {
    csvWriter = new CSVWriter(new BufferedWriter(new FileWriter(config.outputPath)))
    csvWriter.writeNext(Array("Query", "Algorithm", "Time", "WCOJTime", "copy", "mat"))
  }

  private def reportResults(results: Seq[QueryResult]): Unit = {
    require(results.size == config.reps)

    require(results.map(_.count).toSet.size == 1)
    require(results.map(_.algorithm).toSet.size == 1)
    require(results.map(_.query).toSet.size == 1)
    val time = Utils.avg(results.map(_.time))
    val count = results.head.count

    var wcojTime = 0.0
    var copyTime = 0.0
    var materializationTime = 0.0

    if (results.head.algorithm == WCOJ) {
      val wcojResults = results.map(_.asInstanceOf[WCOJQueryResult])

      wcojTime = Utils.avg(wcojResults.map(_.wcojTime))
      copyTime = Utils.avg(wcojResults.map(_.copyTime))
      materializationTime = Utils.avg(wcojResults.map(_.materializationTime))
    }

    println(s"Using ${results.head.algorithm}, ${results.head.query} took $time in average over ${config.reps} repetitions (result size $count).")
    if (results.head.algorithm == WCOJ) {
      println(s"WCOJ took $wcojTime, copying took $copyTime took $materializationTime")
    }

    println("")

    csvWriter.writeNext(Array[String](results.head.query.toString, results.head.algorithm.toString, "%03.2f".format(time), "%03.2f".format(wcojTime), "%03.2f".format(copyTime), "%03.2f".format(materializationTime)))
    csvWriter.flush()
  }

  private def runQueries() = {
    for (q <- config.queries) {
      runQuery(config.algorithms, q)
    }
  }

  private def runQuery(algorithms: Seq[Algorithm], query: Query): Unit = {
    for (algoritm <- algorithms) {
      val queryDataFrame = algoritm match {
        case BinaryJoins => {
          query match {
            case Clique(s) => {
              Queries.cliqueBinaryJoins(s, sp, ds)
            }
            case Cycle(s) => {
              Queries.cycleBinaryJoins(s, ds)
            }
            case PathQuery(s, selectivity) => {
              val (ns1, ns2) = Queries.pathQueryNodeSets(ds, selectivity)
              Queries.pathBinaryJoins(s, ds, ns1, ns2)
            }
            case DiamondQuery() => {
              Queries.diamondBinaryJoins(ds)
            }
            case HouseQuery() => {
              Queries.houseBinaryJoins(sp, ds)
            }
          }
        }
        case WCOJ => {
          query match {
            case Clique(s) => {
              Queries.cliquePattern(s, ds)
            }
            case Cycle(s) => {
              Queries.cyclePattern(s, ds)
            }
            case PathQuery(s, selectivity) => {
              val (ns1, ns2) = Queries.pathQueryNodeSets(ds, selectivity)
              Queries.pathPattern(s, ds, ns1, ns2)
            }
            case DiamondQuery() => { // TODO remove diamond query, it is a four cycle
              Queries.diamondPattern(ds)
            }
            case HouseQuery() => {
              Queries.housePattern(ds)
            }
          }
        }
      }

      val results = ListBuffer[QueryResult]()

      for (i <- 1 to config.reps) {
        wcojTimes.clear() // TODO make single value
        materializationTimes.clear()
        copyTimes.clear()
        System.gc()
        print(".")
        val start = System.nanoTime()
        val count = queryDataFrame.count()
        val end = System.nanoTime()
        val time = (end - start).toDouble / 1000000000
        //        println(s"$algoritm $count")
        algoritm match {
          case WCOJ => {
            results += WCOJQueryResult(query, count, time, wcojTimes.head, copyTimes.head, materializationTimes.head)
          }
          case BinaryJoins => {
            results += BinaryQueryResult(query, count, time)
          }
        }
      }
      println()

      reportResults(results)
    }
  }


  private def setupMetricListener(wcojTimes: ListBuffer[Double], materializationTimes: ListBuffer[Double], copyTimes: ListBuffer[Double]): Unit = {
    sp.sparkContext.addSparkListener(new SparkListener {
      override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
        var materializationTimeTotal = 0L

        stageCompleted.stageInfo.accumulables.foreach({
          case (_, AccumulableInfo(_, Some(name), _, Some(value), _, _, _)) => {
            if (name.startsWith("wcoj time")) {
              wcojTimes += value.asInstanceOf[Long].toDouble / 1000
            } else if (name.startsWith("materialization time")) {
              materializationTimeTotal += value.asInstanceOf[Long]
            } else if (name.startsWith("copy")) {
              copyTimes += value.asInstanceOf[Long].toDouble / 1000
            }
          }
        })

        if (materializationTimeTotal != 0) {
          materializationTimes += materializationTimeTotal.toDouble / 1000
        }
      }
    })

  }

}
