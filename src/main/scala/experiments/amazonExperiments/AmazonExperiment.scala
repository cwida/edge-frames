package experiments.amazonExperiments

import java.util.Timer

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import sparkIntegration.WCOJ2WCOJExec
import sparkIntegration.implicits._

import scala.io.StdIn
import experiments.Datasets.loadAmazonDataset
import experiments.Queries.{trianglePattern,triangleBinaryJoins}

object AmazonExperiment extends App  {

  def setupSpark(): SparkSession = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("Spark test")
      .set("spark.executor.memory", "2g")
      .set("spark.driver.memory", "2g")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    spark.experimental.extraStrategies = (Seq(WCOJ2WCOJExec) ++ spark.experimental.extraStrategies)
    spark
  }

  val sp = setupSpark()
  import sp.implicits._

  println("Read dataset")
  val df = loadAmazonDataset(sp).limit(1000)

  println("Starting binary triangle join")
  val startBinary = System.currentTimeMillis()
  val countBySpark = triangleBinaryJoins(sp, df)
  val endBinary = System.currentTimeMillis()
  println($"Count by binary joins $countBySpark took ${(endBinary - startBinary) / 1000}")

  println("Starting WCOJ triangle join")
  val startWCOJ = System.currentTimeMillis()
  val result = trianglePattern(df)
  result.explain(true)
  val WCOJCount = result.count()
  val endWCOJ = System.currentTimeMillis()
  println(s"Count by WCOJ join: ${WCOJCount} took ${(endWCOJ - startWCOJ) / 1000}")


  StdIn.readLine("Should stop?")
  sp.stop()

}
