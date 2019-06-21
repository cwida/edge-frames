package sharesSimulator

import java.io.{BufferedWriter, File, FileWriter}

import au.com.bytecode.opencsv.CSVWriter
import com.univocity.parsers.csv.CsvWriter
import experiments.{AmazonCoPurchase, DatasetType, Datasets, Query}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OParser
import sparkIntegration.WCOJ2WCOJExec

case class ExperimentConfig(
                             datasetType: DatasetType = AmazonCoPurchase,
                             datasetFilePath: String = ".",
                             queries: Seq[Query] = Seq.empty,
                             outputPath: File = new File("."),
                             workers: Int = 1
                           )


object Simulator extends App {
  val config: ExperimentConfig = parseArgs().orElse(throw new IllegalArgumentException("Couldn't parse args")).get

  val sp = setupSpark()


  val ds = loadDataset().limit(10000)

  for (q <- config.queries) {
    val workCount = simulateQuery(q)
    logResult(q, workCount)
  }

  sp.stop()

  private def parseArgs(): Option[ExperimentConfig] = {
    import experiments.Queries.queryRead
    import experiments.Datasets.datasetTypeRead

    val builder = OParser.builder[ExperimentConfig]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("shares-simulator"),
        head("shares-simulator", "0.1"),
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
        opt[Int]('w', "workers")
          .required()
          .action((x, c) => c.copy(workers = x))
      )
    }
    OParser.parse(parser1, args, ExperimentConfig())
  }

  private def setupSpark(): SparkSession = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("Spark test")
      .set("spark.executor.memory", "40g")
      .set("spark.driver.memory", "40g")
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    spark.experimental.extraStrategies = (Seq(WCOJ2WCOJExec) ++ spark.experimental.extraStrategies)
    spark
  }

  private def loadDataset(): DataFrame = {
    val dt = config.datasetType

    println(s"Loading ${dt} dataset from ${config.datasetFilePath}")
    val d = config.datasetType.loadDataset(config.datasetFilePath, sp).cache()
//    val count = d.count() // Trigger dataset caching
//    println(s"Running on $count rows")
    d
  }

  private def simulateQuery(query: Query): DataFrame = {

    import sp.implicits._
    val hypercube = new Hypercube(config.workers, query)
    val mappedDataset = hypercube.calculateWorkers(ds).toDF().cache()

    mappedDataset.show(100)
    println(mappedDataset.count())

    mappedDataset.groupBy("_1").count().sort("_1")
  }

  private def logResult(q: Query, workCount: DataFrame): Unit = {
    workCount.coalesce(1).write.csv(config.outputPath + s"/${q.name}")
  }
}