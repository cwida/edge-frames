package correctnessTesting

import experiments.Algorithm
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, WCOJFunctions}
import org.scalatest.{FlatSpec, Matchers}
import testing.{SparkTest, Utils}
import experiments.Queries._
import org.apache.spark.mllib.tree.configuration.Algo.Algo
import sparkIntegration.implicits._

class CorrectnessTest extends FlatSpec with Matchers with SparkTest {

  private val FAST = true
  if (FAST) {
    System.err.println("Running correctness test in fast mode")
  }

  def assertRDDEqual(a: RDD[Row], e: RDD[Row]) = {
    val aExtras = a.subtract(e)
    val eExtras = e.subtract(a)

    val aExtrasEmpty = aExtras.isEmpty()
    val eExtrasEmpty = eExtras.isEmpty()

    if (!(aExtrasEmpty && eExtrasEmpty)) {
      println("actual contains following extra rows: ")
      Utils.printSeqRDD(50, aExtras.map(r => r.toSeq))
      println("expected contains following extra rows: ")
      Utils.printSeqRDD(50, eExtras.map(r => r.toSeq))
    }

    aExtrasEmpty should be(true)
    eExtrasEmpty should be(true)
  }

  def assertRDDSetEqual(a: RDD[Row], e: RDD[Row], setSize: Int) = {
    val aSet = a.map(r => r.toSeq.toSet)
    val eSet = e.map(r => r.toSeq.toSet)

    aSet.filter(_.size != setSize).isEmpty() should be (true)
    eSet.filter(_.size != setSize).isEmpty() should be (true)


    val aExtra = aSet.subtract(eSet)
    val eExtra = eSet.subtract(aSet)

    val empty1 = aExtra.isEmpty()
    val empty2 = eExtra.isEmpty()

    if (!(empty1 && empty2)) {
      println("actual contains following extra rows: ")
      Utils.printSetRDD(50, aExtra)
      println("expected contains following extra rows: ")
      Utils.printSetRDD(50, eExtra)
    }

    empty1 should be (true)
    empty2 should be (true)
  }

  private def getPathQueryDataset(ds: DataFrame): DataFrame = {
    // TODO remove once path queries are fast enough
    if (FAST) {
      ds
    } else {
      ds.limit(1000)
    }
  }

  def sparkPathJoins(rawDataset: DataFrame): Unit = {
    val ds = if (FAST) {
      rawDataset.limit(1000)
    } else {
      rawDataset
    }

    val (nodeSet1, nodeSet2) = pathQueryNodeSets(ds)
    nodeSet1.cache()
    nodeSet2.cache()
    nodeSet1.count()
    nodeSet2.count()

    "two-paths" should "be the same" in {
      val pathQuerySet = getPathQueryDataset(ds)

      val e = pathBinaryJoins(2, pathQuerySet, nodeSet1, nodeSet2).cache()
      val a = pathPattern(2, pathQuerySet, nodeSet1, nodeSet2).cache()

      assertRDDEqual(a.rdd, e.rdd)
      a.isEmpty should be(false)
      e.isEmpty should be(false)
    }

    "three-paths" should "be the same" in {
      val pathQuerySet = getPathQueryDataset(ds)

      val e = pathBinaryJoins(3, pathQuerySet, nodeSet1, nodeSet2).cache()
      val a = pathPattern(3, pathQuerySet, nodeSet1, nodeSet2).cache()

      assertRDDEqual(a.rdd, e.rdd)
      a.isEmpty should be(false)
      e.isEmpty should be(false)
    }

    "four-path" should "be the same" in {
      val pathQuerySet = getPathQueryDataset(ds)

      val e = pathBinaryJoins(4, pathQuerySet, nodeSet1, nodeSet2)
      val a = pathPattern(4, pathQuerySet, nodeSet1, nodeSet2)

      assertRDDEqual(a.rdd, e.rdd)

      a.isEmpty should be(false)
      e.isEmpty should be(false)
    }
  }

  def sparkTriangleJoins(rawDataset: DataFrame): Unit = {
    val ds = if (FAST) {
      rawDataset.limit(1000)
    } else {
      rawDataset
    }

    "triangles" should "be the same" in {
      assertRDDEqual(cliquePattern(3, ds).rdd, cliqueBinaryJoins(3, sp, ds).rdd)
    }

    "The variable ordering" should "not matter" in {
      // Cannot use Queries.findPattern(3, ds, false) here because I do not support smallerThanFilter for any variable ordering
      // but the global one and a, b, c and c, a, b have different results under this condition.
      val normalVariableOrdering = cliquePattern(3, ds, useDistinctFilter = true).cache()
      val otherVariableOrdering = ds.findPattern(
        """
          |(a) - [] -> (b);
          |(b) - [] -> (c);
          |(a) - [] -> (c)
          |""".stripMargin, List("c", "a", "b"), distinctFilter = true).cache()

      val otherReordered = otherVariableOrdering.select("a", "b", "c")

      assertRDDEqual(otherReordered.rdd, normalVariableOrdering.rdd)
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

  def sparkCliqueJoins(rawDataset: DataFrame): Unit = {
    val ds = if (FAST) {
      rawDataset.limit(1000)
    } else {
      rawDataset
    }

    "Four clique" should "be the same" in {
      val a = cliquePattern(4, ds)
      val e = cliqueBinaryJoins(4, sp, ds)

      assertRDDEqual(a.rdd, e.rdd)
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
  }

  def sparkCycleJoins(rawDataset: DataFrame): Unit = {
    val ds = if (FAST) {
      rawDataset.limit(1000)
    } else {
      rawDataset
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

  def sparkOtherJoins(rawDataset: DataFrame): Unit = {
    val ds = if (FAST) {
      rawDataset.limit(1000)
    } else {
      rawDataset
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

    "kite" should "be the same" in {
      val a = kiteBinary(sp, ds)
      val e = kitePattern(ds)

      assertRDDEqual(a.rdd, e.rdd)
    }
  }
}   
