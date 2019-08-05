package experiments

import java.io.{BufferedWriter, File, FileWriter}
import java.text.{DecimalFormat, NumberFormat}
import java.util.{Formatter, Locale}

import au.com.bytecode.opencsv.CSVWriter
import experiments.metrics.Metrics
import leapfrogTriejoin.MaterializingLeapfrogJoin
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{AccumulableInfo, SparkListener, SparkListenerStageCompleted}
import org.apache.spark.sql.{DataFrame, SparkSession, WCOJFunctions}
import partitioning.{AllTuples, Partitioning}
import scopt.OParser
import sparkIntegration.{WCOJ2WCOJExec, WCOJConfiguration}

import scala.collection.mutable.ListBuffer

object Readers {
  implicit def algorithmRead: scopt.Read[Algorithm] = {
    scopt.Read.reads({
      case "WCOJ" => {
        WCOJ
      }
      case "graphWCOJ" => {
        GraphWCOJ
      }
      case "bin" => {
        BinaryJoins
      }
      case _ => {
        throw new IllegalArgumentException("Algorithm can be only `WCOJ`, `graphWCOJ` or `bin`")
      }
    })
  }
}

sealed trait Algorithm {
}

sealed trait WCOJAlgorithm extends Algorithm {

}

case object WCOJ extends WCOJAlgorithm {
}

case object GraphWCOJ extends WCOJAlgorithm {
}

case object BinaryJoins extends Algorithm {
}

// TODO migrate to Datasets
sealed trait DatasetType {
  def loadDataset(filePath: String, sp: SparkSession): DataFrame
}

case object AmazonCoPurchase extends DatasetType {
  override def loadDataset(filePath: String, sp: SparkSession): DataFrame = {
    Datasets.loadAmazonDataset(filePath, sp)
  }
}

case object SNB extends DatasetType {
  override def loadDataset(filePath: String, sp: SparkSession): DataFrame = {
    Datasets.loadSNBDataset(sp, filePath)
  }
}

case object LiveJournal2010 extends DatasetType {
  override def loadDataset(filePath: String, sp: SparkSession): DataFrame = {
    Datasets.loadLiveJournalDataset(sp, filePath)
  }
}

case object TwitterSnapEgo extends DatasetType {
  override def loadDataset(filePath: String, sp: SparkSession): DataFrame = {
    Datasets.loadTwitterSnapEgo(sp, filePath)
  }
}

case object GoogleWeb extends DatasetType {
  override def loadDataset(filePath: String, sp: SparkSession): DataFrame = {
    Datasets.loadGoogleWebGraph(filePath, sp)
  }
}

case class ExperimentConfig(
                             algorithms: Seq[Algorithm] = Seq(WCOJ, BinaryJoins),
                             datasetType: DatasetType = AmazonCoPurchase,
                             datasetFilePath: String = ".",
                             queries: Seq[Query] = Seq.empty,
                             parallelismLevels: Seq[Int] = Seq(1),
                             partitionings: Seq[Partitioning] = Seq(AllTuples()),
                             workers: Int = 1,
                             outputPath: File = new File("."),
                             reps: Int = 1,
                             limitDataset: Int = -1,
                             comment: String = "",
                             materializeLeapfrogs: Boolean = true
                           )


sealed trait QueryResult {
  def algorithm: Algorithm

  def query: Query

  def count: Long

  def time: Double

  def parallelism: Int
}

case class BinaryQueryResult(query: Query, parallelism: Int, count: Long, time: Double) extends QueryResult {
  override def algorithm: Algorithm = {
    BinaryJoins
  }
}

case class WCOJQueryResult(algorithm: Algorithm,
                           query: Query,
                           partitioning: Partitioning,
                           parallelism: Int,
                           count: Long,
                           time: Double,
                           wcojTime: Double,
                           copyTime: Double,
                           materializationTime: Option[Double]) extends QueryResult {
}

// TODO exclude count times for Spark joins (time after the join)
object ExperimentRunner extends App {
  val f = new Formatter(Locale.US)
  val formatter = NumberFormat.getInstance(Locale.US).asInstanceOf[DecimalFormat]
  val symbols = formatter.getDecimalFormatSymbols

  symbols.setGroupingSeparator(' ')
  symbols.setDecimalSeparator('.')
  formatter.setDecimalFormatSymbols(symbols)

  val config: ExperimentConfig = parseArgs().orElse(throw new IllegalArgumentException("Couldn't parse args")).get

  println("Setting up Spark")
  val sp = setupSpark()
  val wcojConfig = WCOJConfiguration(sp)

  val ds = loadDataset()

  var csvWriter: CSVWriter = null

  setupResultReporting()

  require(!config.materializeLeapfrogs || !config.algorithms.contains(WCOJ), "Cannot use materializing Leapfrog joins with WCOJ, yet.")
  wcojConfig.setShouldMaterialize(config.materializeLeapfrogs)

  cacheGraphBroadcast()

  runQueries()

  scala.io.StdIn.readLine("Stop?")

  sp.stop()

  private def parseArgs(): Option[ExperimentConfig] = {
    import Queries.queryRead
    import Datasets.datasetTypeRead
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
        opt[Seq[Int]]('e', "parallelism")
          .valueName("<parallelism-level1>,<parallelism-level1>...")
          .action((x, c) => c.copy(parallelismLevels = x)),
        opt[Seq[Partitioning]]('p', "partitioning")
          .valueName("<partitioning1>,<partitioning2>...")
          .action((x, c) => c.copy(partitionings = x)),
        opt[Int]('w', "workers")
          .valueName("<workers:Int>")
          .action((x, c) => c.copy(workers = x)),
        opt[Int]('l', "limit")
          .optional
          .action((x, c) => c.copy(limitDataset = x)),
        opt[Int]('r', "reps")
          .optional()
          .action((x, c) => c.copy(reps = x)),
        opt[String]('c', "comment")
          .optional()
          .action((x, c) => c.copy(comment = x)),
        opt[Boolean]('m', "materializeLeapfrogs")
          .optional()
          .action((x, c) => c.copy(materializeLeapfrogs = x))
      )
    }
    OParser.parse(parser1, args, ExperimentConfig())
  }

  private def setupSpark(): SparkSession = {
    val conf = new SparkConf()
      .setMaster(s"local[${config.workers}]")
      .setAppName("Spark test")
      .set("spark.executor.memory", "40g")
      .set("spark.driver.memory", "40g")
//      .set("spark.cores.max", "1")
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
    var d = config.datasetType.loadDataset(config.datasetFilePath, sp)
    if (config.limitDataset != -1) {
      d = d.limit(config.limitDataset)
    }
    d = d.cache()

    val count = d.count() // Trigger dataset caching
    println(s"Running on $count rows")
    d
  }

  private def setupResultReporting(): Unit = {
    csvWriter = new CSVWriter(new BufferedWriter(new FileWriter(config.outputPath)), ',', 0)

    csvWriter.writeNext(Array(s"# Dataset: ${config.datasetType} ${ds.count()} ${config.datasetFilePath}"))
    csvWriter.writeNext(Array(s"# Repetitions: ${config.reps}"))
    csvWriter.writeNext(Array(s"# Git commit: ${BuildInfo.gitCommit}"))
    csvWriter.writeNext(Array(s"# Materializing Leapfrogjoins: ${if (config.materializeLeapfrogs) "Enabled" else "Disabled"}"))
    csvWriter.writeNext(Array(s"# Comment: ${config.comment}"))
    csvWriter.writeNext(Array("Query", "Algorithm", "Partitioning", "Parallelism", "Count", "Time", "WCOJTime", "copy", "mat"))
  }

  private def reportResults(results: Seq[QueryResult]): Unit = {
    require(results.size == config.reps)

    require(results.map(_.count).toSet.size == 1)
    require(results.map(_.algorithm).toSet.size == 1)
    require(results.map(_.query).toSet.size == 1)
    require(results.map(_.parallelism).toSet.size == 1)

    val algorithm = results.head.algorithm
    val query = results.head.query
    val count = results.head.count
    val parallelism = results.head.parallelism

    for (result <- results) {
      val time = result.time

      var wcojTime = 0.0
      var copyTime = 0.0
      var materializationTime = 0.0

      var partitioning = "Spark"

      if (Seq(GraphWCOJ, WCOJ).contains(algorithm)) {
        val wcojResult = result.asInstanceOf[WCOJQueryResult]

        wcojTime = wcojResult.wcojTime
        copyTime = wcojResult.copyTime
        materializationTime = wcojResult.materializationTime match {
          case Some(t) => t
          case None => 0
        }

        partitioning = wcojResult.partitioning.toString
      }

      csvWriter.writeNext(Array[String](
        query.toString,
        algorithm.toString,
        partitioning,
        parallelism.toString,
        String.format(Locale.GERMAN, "%,013d", count.asInstanceOf[Object]),
        String.format(Locale.US, "%.2f", time.asInstanceOf[Object]),
        String.format(Locale.US, "%.2f", wcojTime.asInstanceOf[Object]),
        String.format(Locale.US, "%.2f", copyTime.asInstanceOf[Object]),
        String.format(Locale.US, "%.2f", materializationTime.asInstanceOf[Object])))
      csvWriter.flush()
    }

    println(s"Using $algorithm, $query took ${Utils.avg(results.map(_.time))} in average over ${config.reps} repetitions (result size $count).")
    if (Seq(GraphWCOJ, WCOJ).contains(algorithm)) {
      val wcojResults = results.map(_.asInstanceOf[WCOJQueryResult])
      println(s"WCOJ took ${Utils.avg(wcojResults.map(_.wcojTime))}, copying took ${Utils.avg(wcojResults.map(_.copyTime))} took.")
    }
    println("")
  }


  private def cacheGraphBroadcast(): Unit = {
    for (algoritm <- config.algorithms) {
      algoritm match {
        case BinaryJoins | WCOJ => {
          // Do nothing
        }
        case GraphWCOJ => {
          Metrics.masterTimers.clear()
          System.gc()
          wcojConfig.setJoinAlgorithm(algoritm.asInstanceOf[WCOJAlgorithm])
          Queries.cliquePattern(3, ds).count() // Trigger caching

          println(s"GraphWCOJ broadcast materialization took: ${Metrics.masterTimers("materializationTime").toDouble / 1e9} seconds")
          reportMaterializationTime(Metrics.masterTimers("materializationTime").toDouble / 1e9, algoritm)
        }
      }
    }
  }

  private def reportMaterializationTime(time: Double, algorithm: Algorithm): Unit = {
    csvWriter.writeNext(Array(s"# Materialization time (GraphWCOJ):" + String.format(Locale.US, "%.2f", time.asInstanceOf[Object], algorithm
      .toString)))
  }

  private def runQueries(): Unit = {
    for (q <- config.queries) {
      runQuery(config.algorithms, q)
    }
  }

  private def runQuery(algorithms: Seq[Algorithm], query: Query): Unit = {
    for (pl <- config.parallelismLevels) {
      wcojConfig.setParallelism(pl)
      for (p <- config.partitionings) {
        wcojConfig.setPartitioning(p)
        for (algoritm <- algorithms) {
          val queryDataFrame = algoritm match {
            case BinaryJoins => {
              query.applyBinaryQuery(ds, sp)
            }
            case WCOJ | GraphWCOJ => {
              wcojConfig.setJoinAlgorithm(algoritm.asInstanceOf[WCOJAlgorithm])
              query.applyPatternQuery(ds)
            }
          }

          val results = ListBuffer[QueryResult]()

          for (i <- 1 to config.reps) {
            System.gc()
            print(".")
            val start = System.nanoTime()
            val count = queryDataFrame.count()
            val end = System.nanoTime()
            val time = (end - start).toDouble / 1e9

            algoritm match {
              case WCOJ | GraphWCOJ => {
                val joinTimes = Metrics.getTimes("wcoj_join_time")
                println(joinTimes.mkString(", "))
                val copyTimes = Metrics.getTimes("copy_time")
                val materializationTimes = Metrics.getTimes("materializationTime")

                results += WCOJQueryResult(algoritm, query, p, pl, count, time,
                  joinTimes.head._2.toDouble / 1e9, copyTimes.head._2.toDouble / 1e9,
                  None)  // TODO materialization times
              }
              case BinaryJoins => {
                results += BinaryQueryResult(query, pl, count, time)
              }
            }
          }
          println()

          reportResults(results)
        }
      }
    }
  }
}