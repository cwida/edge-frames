package correctnessTesting

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.scalatest.{FlatSpec, Matchers}
import sparkIntegration.WCOJ2WCOJExec
import sparkIntegration.implicits._
import testing.SparkTest
import experiments.Queries._

class AmazonDatasetTriangleQuery extends FlatSpec with Matchers with SparkTest {
  val DATASET_PATH = "file:///home/per/workspace/master-thesis/datasets"
  val AMAZON_DATASET_FILE_NAME = "amazon-0302.txt"
  val OFFICIAL_NUMBERS_OF_TRIANGLES = 717719L

  val FAST = true
  if (FAST) {
    System.err.println("Running correctness test in fast mode")
  }

  val ds = loadAmazonDataset().cache()

  val goldStandardTriangles = triangleBinaryJoins(sp, ds).cache()
  val actualResultTriangles = trianglePattern(ds).cache()

  val (nodeSet1, nodeSet2) = pathQueryNodeSets(ds)
  nodeSet1.cache()
  nodeSet2.cache()

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

  "WCOJ implementation" should "find the same two-paths as Spark's original joins" in {
    val a = twoPathPattern(ds, nodeSet1, nodeSet2).cache()
    val e = twoPathBinaryJoins(ds, nodeSet1, nodeSet2).cache()

    val diff = a.rdd.subtract(e.rdd)
    diff.isEmpty() should be (true)
    a.isEmpty should be (false)
    e.isEmpty should be (false)
  }

  "WCOJ implementation" should "find the same three-paths as Spark's original joins" in {
    val a = threePathPattern(ds, nodeSet1, nodeSet2).cache()
    val e = threePathBinaryJoins(ds, nodeSet1, nodeSet2).cache()

    val diff = a.rdd.subtract(e.rdd)
    diff.isEmpty() should be (true)
    a.isEmpty should be (false)
    e.isEmpty should be (false)
  }

  "WCOJ implementation" should "find the same four-paths as Spark's original joins" in {
    val startA = System.nanoTime()
    val a = fourPathPattern(ds, nodeSet1, nodeSet2).cache()
    print("a ", a.count())
    val endA = System.nanoTime() - startA
    println("WCOJ four-path ", endA / 1000000000)

    val startE = System.nanoTime()
    val e = fourPathBinaryJoins(ds, nodeSet1, nodeSet2).cache()
    println("e", e.count())
    val endE = System.nanoTime() - startE
    println("Spark four-path ", endE / 1000000000)

    val diff = a.rdd.subtract(e.rdd)
    diff.isEmpty() should be (true)
    a.isEmpty should be (false)
    e.isEmpty should be (false)
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
