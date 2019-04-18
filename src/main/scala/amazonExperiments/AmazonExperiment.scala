package amazonExperiments

import java.util.Timer

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import sparkIntegration.WCOJ2WCOJExec
import sparkIntegration.implicits._

import scala.io.StdIn

object AmazonExperiment extends App  {

  val DATASET_PATH = "file:///home/per/workspace/master-thesis/datasets"
  val AMAZON_DATASET_FILE_NAME = "amazon-0302.txt"

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

  def loadAmazonDataset(): DataFrame = {
    val df = sp.read
      .format("csv")
      .option("delimiter", "\t")
      .option("inferSchema", true)
      .option("comment", "#")
      .csv(List(DATASET_PATH, AMAZON_DATASET_FILE_NAME).mkString("/"))
//      .limit(30000)
      .withColumnRenamed("_c0", "src")
      .withColumnRenamed("_c1", "dst")
//    println(df.count())
//    println(df.show(5))
    df
  }

  def findTriangles(spark: SparkSession, rel: DataFrame): Long = {
    import spark.implicits._

    val r = rel
      .withColumnRenamed("src", "p1")
      .withColumnRenamed("dst", "p2")

    val duos = r.as("k1")
      .joinWith(r.as("k2"), $"k1.p2" === $"k2.p1")
    val triangles = duos.joinWith(r.as("k3"),
      condition = $"_2.p2" === $"k3.p2" && $"_1.p1" === $"k3.p1")
    triangles.explain(true)
    triangles.count()
  }

  val sp = setupSpark()
  import sp.implicits._

  println("Read dataset")
  val df = loadAmazonDataset()

//  println("Starting binary triangle join")
//  val startBinary = System.currentTimeMillis()
//  val countBySpark = findTriangles(sp, df)
//  val endBinary = System.currentTimeMillis()
//  println($"Count by binary joins $countBySpark took ${(endBinary - startBinary) / 1000}")

  println("Starting WCOJ triangle join")
  val startWCOJ = System.currentTimeMillis()
  val result = df.findPattern(
    """
      |(a) - [] -> (b);
      |(b) - [] -> (c);
      |(a) - [] -> (c)
      |""".stripMargin, List("a", "b", "c"))
  result.explain(true)
  val WCOJCount = result.count()
  val endWCOJ = System.currentTimeMillis()
//  result.collect()
  println(s"Count by WCOJ join: ${WCOJCount} took ${(endWCOJ - startWCOJ) / 1000}")


  StdIn.readLine("Should stop?")
  sp.stop()

}
