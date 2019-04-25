package experiments

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import sparkIntegration.WCOJ2WCOJExec

import scala.io.StdIn

trait GenericExperiment {

  def loadDataset(sp: SparkSession): DataFrame
  def runWCOJ(sp: SparkSession, dataSet: DataFrame): Long
  def runBinaryJoins(sp: SparkSession, dataSet: DataFrame): Long

  def run(): Unit = {
    val sp = setupSpark()

    println("Loading dataset")
    val ds = loadDataset(sp)
    ds.cache()

    println("Starting binary join")
    val startBinary = System.currentTimeMillis()
    val countBySpark = runBinaryJoins(sp, ds)
    val endBinary = System.currentTimeMillis()
    println(s"Count by binary joins $countBySpark took ${(endBinary - startBinary) / 1000}")

    println("Starting WCOJ triangle join")
    val startWCOJ = System.currentTimeMillis()
    val WCOJCount = runWCOJ(sp, ds)
    val endWCOJ = System.currentTimeMillis()
    println(s"Count by WCOJ join: $WCOJCount took ${(endWCOJ - startWCOJ) / 1000}")


    StdIn.readLine("Should stop?")
    sp.stop()
  }

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


}
