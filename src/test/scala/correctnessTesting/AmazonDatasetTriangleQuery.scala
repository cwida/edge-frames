package correctnessTesting

import org.scalatest.{FlatSpec, Matchers}
import sparkIntegration.implicits._
import testing.SparkTest
import experiments.Queries._
import experiments.Datasets.loadAmazonDataset
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class AmazonDatasetTriangleQuery extends FlatSpec with Matchers with SparkTest {
  val OFFICIAL_NUMBERS_OF_TRIANGLES = 717719L

  val FAST = true
  if (FAST) {
    System.err.println("Running correctness test in fast mode")
  }

  val ds = if (FAST) {
    loadAmazonDataset(sp).limit(200).cache()
  } else {
    loadAmazonDataset(sp).cache()
  }
  println(ds.count())

  val goldStandardTriangles = triangleBinaryJoins(sp, ds).cache()
  val actualResultTriangles = trianglePattern(ds).cache()

  val (nodeSet1, nodeSet2) = pathQueryNodeSets(ds)
  nodeSet1.cache()
  nodeSet2.cache()
  println(nodeSet1.count(), nodeSet2.count())


  private def assertRDDEqual[A: ClassTag](rdd1: RDD[A], rdd2: RDD[A]) = {
    rdd1.subtract(rdd2).isEmpty() should be (true)
    rdd2.subtract(rdd1).isEmpty() should be (true)
  }


  "WCOJ implementation" should "find the same two-paths as Spark's original joins" in {
    val a = twoPathPattern(ds, nodeSet1, nodeSet2).cache()
    val e = twoPathBinaryJoins(ds, nodeSet1, nodeSet2).cache()

    val diff = a.rdd.subtract(e.rdd)
    diff.isEmpty() should be(true)
    a.isEmpty should be(false)
    e.isEmpty should be(false)
  }

  "WCOJ implementation" should "find the same three-paths as Spark's original joins" in {
    val a = threePathPattern(ds, nodeSet1, nodeSet2).cache()
    val e = threePathBinaryJoins(ds, nodeSet1, nodeSet2).cache()

    assertRDDEqual(a.rdd, e.rdd)
    a.isEmpty should be(false)
    e.isEmpty should be(false)
  }

  "WCOJ implementation" should "find the same four-paths as Spark's original joins" in {
    val startE = System.nanoTime()
    val e = fourPathBinaryJoins(ds, nodeSet1, nodeSet2)
    val countE = e.count()
    val endE = System.nanoTime() - startE
    println("e", countE)
    println("Spark four-path ", endE.toDouble / 1000000000)

    val startA = System.nanoTime()
    val a = fourPathPattern(ds, nodeSet1, nodeSet2)
    val countA = a.count()
    val endA = System.nanoTime() - startA
    print("a ", countA)
    println("WCOJ four-path ", endA.toDouble / 1000000000)

    assertRDDEqual(a.rdd, e.rdd)

    a.isEmpty should be(false)
    e.isEmpty should be(false)
  }

  "WCOJ implementation" should "find same triangles as Spark's original joins" in {
    actualResultTriangles.count() should equal(goldStandardTriangles.count())

    assertRDDEqual(actualResultTriangles.rdd, goldStandardTriangles.rdd)
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

    assertRDDEqual(otherReordered.rdd, actualResultTriangles.rdd)
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

}
