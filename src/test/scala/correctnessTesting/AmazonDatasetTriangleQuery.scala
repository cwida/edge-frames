package correctnessTesting

import experiments.Datasets.loadAmazonDataset
import experiments.Queries._
import org.apache.spark.sql.DataFrame
import sparkIntegration.implicits._

class AmazonDatasetTriangleQuery extends CorrectnessTest {
  val DATASET_PATH = "/home/per/workspace/master-thesis/datasets/amazon-0302"

  val FAST = false
  if (FAST) {
    System.err.println("Running correctness test in fast mode")
  }

  val ds = if (FAST) {
    loadAmazonDataset(DATASET_PATH, sp).limit(1000).cache()
  } else {
      loadAmazonDataset(DATASET_PATH, sp).cache()
  }
  println(s"Testing on the first ${ds.count()} edges of the Amazon set")

  val goldStandardTriangles = cliqueBinaryJoins(3, sp, ds).cache()
  val actualResultTriangles = cliquePattern(3, ds).cache()

  val (nodeSet1, nodeSet2) = pathQueryNodeSets(ds)
  nodeSet1.cache()
  nodeSet2.cache()


  private def getPathQueryDataset(): DataFrame = {
    // TODO remove once path queries are fast enough
    if (FAST) {
      ds
    } else {
      ds.limit(1000)
    }
  }

  "WCOJ implementation" should "find the same two-paths as Spark's original joins" in {
    val ds = getPathQueryDataset()

    val e = pathBinaryJoins(2, ds, nodeSet1, nodeSet2).cache()
    val a = pathPattern(2, ds, nodeSet1, nodeSet2).cache()

    assertRDDEqual(a.rdd, e.rdd)
    a.isEmpty should be(false)
    e.isEmpty should be(false)
  }

  "WCOJ implementation" should "find the same three-paths as Spark's original joins" in {
    val ds = getPathQueryDataset()

    val e = pathBinaryJoins(3, ds, nodeSet1, nodeSet2).cache()
    val a = pathPattern(3, ds, nodeSet1, nodeSet2).cache()

    assertRDDEqual(a.rdd, e.rdd)
    a.isEmpty should be(false)
    e.isEmpty should be(false)
  }

  "WCOJ implementation" should "find the same four-paths as Spark's original joins" in {
    val ds = getPathQueryDataset()

    val e = pathBinaryJoins(4, ds, nodeSet1, nodeSet2)
    val a = pathPattern(4, ds, nodeSet1, nodeSet2)

    assertRDDEqual(a.rdd, e.rdd)

    a.isEmpty should be(false)
    e.isEmpty should be(false)
  }

  "WCOJ implementation" should "find same triangles as Spark's original joins" in {
    assertRDDEqual(actualResultTriangles.rdd, goldStandardTriangles.rdd)
  }

  "The variable ordering" should "not matter" in {
    val otherVariableOrdering = ds.findPattern(
      """
        |(a) - [] -> (b);
        |(b) - [] -> (c);
        |(a) - [] -> (c)
        |""".stripMargin, List("c", "a", "b"))

    val otherReordered = otherVariableOrdering.select("a", "b", "c")

    assertRDDSetEqual(otherReordered.rdd, actualResultTriangles.rdd, 3)
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

    assertRDDEqual(circular.rdd, goldStandard.rdd)
  }

  "Four clique" should "be the same" in {
    val a = cliquePattern(4, ds)
    val e = cliqueBinaryJoins(4, sp, ds)

    assertRDDEqual(a.rdd, e.rdd)
  }

  "Diamond query" should "be the same" in {
    val a = diamondPattern(ds)
    val e = diamondBinaryJoins(ds)

    assertRDDSetEqual(a.rdd, e.rdd, 4)
  }

  "House query" should "be the same" in {
    val a = housePattern(ds)
    val e = houseBinaryJoins(sp, ds)

    assertRDDSetEqual(a.rdd, e.rdd, 5)
  }

  "5-clique query" should "be the same" in {
    val a = cliquePattern(5, ds)
    val e = cliqueBinaryJoins(5, sp, ds)

    assertRDDEqual(a.rdd, e.rdd)
  }

  "6-clique query" should "be the same" in {
    val a = cliquePattern(6, ds)
    val e = cliqueBinaryJoins(6, sp, ds)

    assertRDDEqual(a.rdd, e.rdd)
  }

  "4-cylce" should "be the same" in {
    val a = cyclePattern(4, ds)
    val e = cycleBinaryJoins(4, ds)

    assertRDDSetEqual(a.rdd, e.rdd, 4)
  }

  "5-cylce" should "be the same" in {
    val a = cyclePattern(5, ds)
    val e = cycleBinaryJoins(5, ds)

    assertRDDSetEqual(a.rdd, e.rdd, 5)
  }

}
