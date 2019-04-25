package correctnessTesting

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FlatSpec, Matchers}
import sparkIntegration.WCOJ2WCOJExec
import sparkIntegration.implicits._
import testing.SparkTest

class AmazonDatasetTriangleQuery extends FlatSpec with Matchers with SparkTest {
  val DATASET_PATH = "file:///home/per/workspace/master-thesis/datasets"
  val AMAZON_DATASET_FILE_NAME = "amazon-0302.txt"
  val OFFICIAL_NUMBERS_OF_TRIANGLES = 717719L
  val FIXED_SEED_1 = 42
  val FIXED_SEED_2 = 220

  val FAST = true
  if (FAST) {
    System.err.println("Running correctness test in fast mode")
  }

  import sp.implicits._

  val ds = loadAmazonDataset().cache()

  val goldStandardTriangles = goldStandardFindTriangles(sp, ds).cache()
  val actualResultTriangles = ds.findPattern(
    """
      |(a) - [] -> (b);
      |(b) - [] -> (c);
      |(a) - [] -> (c)
      |""".stripMargin, List("a", "b", "c"))
    .cache()

  val (nodeSet1, nodeSet2) = twoPathNodeSet(ds)

  def loadAmazonDataset(): DataFrame = {
    val df = sp.read
      .format("csv")
      .option("delimiter", "\t")
      .option("inferSchema", true)
      .option("comment", "#")
      .csv(List(DATASET_PATH, AMAZON_DATASET_FILE_NAME).mkString("/"))
      .withColumnRenamed("_c0", "src")
      .withColumnRenamed("_c1", "dst")
      .repartition(1)
      .cache()
    if (FAST) {
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

  def twoPathNodeSet(rel: DataFrame): (DataFrame, DataFrame) = {
    (rel.selectExpr("src AS a").sample(0.1, FIXED_SEED_1).cache(),
      rel.selectExpr("src AS c").sample(0.1, FIXED_SEED_2).cache())
  }

  def twoPathGoldStandard(rel: DataFrame, nodeSet1: DataFrame, nodeSet2: DataFrame): DataFrame = {
    val relLeft = rel.selectExpr("src AS a", "dst AS b").join(nodeSet1, Seq("a"), "left_semi")
    val relRight = rel.selectExpr("dst AS c", "src AS b").join(nodeSet2, Seq("c"), "left_semi")

    relLeft.join(relRight, Seq("b")).selectExpr("a", "b", "c")
  }

  def twoPathPattern(rel: DataFrame, nodeSet1: DataFrame, nodeSet2: DataFrame) = {
    val twoPath = rel.findPattern(
      """
        |(a) - [] -> (b);
        |(b) - [] -> (c)
      """.stripMargin, Seq("a", "c", "b"))
    // TODO should be done before the join
    twoPath.join(nodeSet1, Seq("a"), "left_semi")
      .join(nodeSet2, Seq("c"), "left_semi")
      .select("a", "b", "c")
  }

  "WCOJ implementation" should "find the same two-paths as Spark's original joins" in {
    val a = twoPathPattern(ds, nodeSet1, nodeSet2).cache()
    val e = twoPathGoldStandard(ds, nodeSet1, nodeSet2).cache()

    val diff = a.rdd.subtract(e.rdd)
    diff.isEmpty() should be (true)
  }


  "WCOJ implementation" should "find same triangles as Spark's original joins" in {
    actualResultTriangles.count() should equal(goldStandardTriangles.count())

    val difference = goldStandardTriangles.rdd.subtract(actualResultTriangles.rdd)
    difference.isEmpty() should be(true)
  }

  "WCOJ implementation" should "produce roughly as many triangles as on the official website" in {
    if (!FAST) {
      val distinct = actualResultTriangles.rdd.map(r => r.toSeq.toSet).distinct(1).count()
      distinct should equal(OFFICIAL_NUMBERS_OF_TRIANGLES +- (OFFICIAL_NUMBERS_OF_TRIANGLES * 0.01).toLong)
    } else {
      fail("Cannot run comparision to original data in FAST mode")
    }
  }

  "The variable ordering" should "not matter" in {
    val otherVariableOrdering = ds.findPattern(
      """
        |(a) - [] -> (b);
        |(b) - [] -> (c);
        |(a) - [] -> (c)
        |""".stripMargin, List("c", "a", "b"))

    val otherReordered = otherVariableOrdering.select("a", "b", "c")

    val diff = actualResultTriangles.rdd.subtract(otherReordered.rdd)
    diff.isEmpty() should be(true)
  }

  "Circular triangles" should "be found correctly" in {
    import sp.implicits._

    val circular = ds.findPattern(
      """
        |(a) - [] -> (b);
        |(b) - [] -> (c);
        |(c) - [] -> (a)
        |""".stripMargin, List("a", "b", "c"))

    val duos = ds.as("R")
      .joinWith(ds.as("S"), $"R.dst" === $"S.src")
    val triangles = duos.joinWith(ds.as("T"),
      condition = $"_2.dst" === $"T.src" && $"_1.src" === $"T.dst")

    val goldStandard = triangles.selectExpr("_2.dst AS a", "_1._1.dst AS b", "_2.src AS c")


    val diff = goldStandard.rdd.subtract(circular.rdd)
    diff.isEmpty() should be(true)
  }

}
