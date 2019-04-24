package correctnessTesting

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FlatSpec, Matchers}
import sparkIntegration.WCOJ2WCOJExec
import sparkIntegration.implicits._

class AmazonDatasetTriangleQuery extends FlatSpec with Matchers {
  val DATASET_PATH = "file:///home/per/workspace/master-thesis/datasets"
  val AMAZON_DATASET_FILE_NAME = "amazon-0302.txt"
  val OFFICIAL_NUMBERS_OF_TRIANGLES = 717719L

  val FAST = false
  if (FAST) {
    System.err.println("Running correctness test in fast mode")
  }

  val sp = setupSpark()
  val df = loadAmazonDataset()

  val goldStandard = goldStandardFindTriangles(sp, df)

  val actualResult = df.findPattern(
    """
      |(a) - [] -> (b);
      |(b) - [] -> (c);
      |(a) - [] -> (c)
      |""".stripMargin, List("a", "b", "c"))

  goldStandard.cache()
  actualResult.cache()

  def setupSpark(): SparkSession = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("Spark test")
      .set("spark.executor.memory", "2g")
      .set("spark.driver.memory", "2g")
      .set("spark.default.parallelism", "1")

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
      .cache()
      .withColumnRenamed("_c0", "src")
      .withColumnRenamed("_c1", "dst")
      .repartition(1)
    if (FAST) {
      // TODO produces bug with 1000
      df.limit(200)
    } else {
      df
    }
  }

  def goldStandardFindTriangles(spark: SparkSession, rel: DataFrame): DataFrame = {
    import spark.implicits._

    val duos = rel.as("R")
      .joinWith(rel.as("S"), $"R.dst" === $"S.src")
    val triangles = duos.joinWith(rel.as("T"),
      condition = $"_2.dst" === $"T.dst" && $"_1.src" === $"T.src")

    triangles.selectExpr("_2.src AS a", "_1._1.dst AS b", "_2.dst AS c")
  }


  "WCOJ implementation" should "find same triangles as Spark's original joins" in {
    actualResult.count() should equal(goldStandard.count())

    val difference = goldStandard.rdd.subtract(actualResult.rdd)
    difference.isEmpty() should be(true)
  }

  "WCOJ implementation" should "produce roughly as many triangles as on the official website" in {
    if (!FAST) {
      val distinct = actualResult.rdd.map(r => r.toSeq.toSet).distinct(1).count()
      distinct should equal(OFFICIAL_NUMBERS_OF_TRIANGLES +- (OFFICIAL_NUMBERS_OF_TRIANGLES * 0.01).toLong)
    } else {
      fail("Cannot run comparision to original data in FAST mode")
    }
  }

  "The variable ordering" should "not matter" in {
    val otherDirection = df.findPattern(
      """
        |(a) - [] -> (b);
        |(b) - [] -> (c);
        |(a) - [] -> (c)
        |""".stripMargin, List("c", "a", "b"))

    val otherReordered = otherDirection.select("a", "b", "c")

    val diff = actualResult.rdd.subtract(otherReordered.rdd)
    diff.isEmpty() should be (true)
  }

}
