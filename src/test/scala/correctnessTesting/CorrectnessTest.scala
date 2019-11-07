package correctnessTesting

import java.lang.Exception

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import experiments.DiamondQuery
import experiments.Queries._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FlatSpec, Matchers}
import sparkIntegration.WCOJConfiguration
import testing.{SparkTest, Utils}
import sparkIntegration.implicits._

object CorrectnessTest {
  var FAST = true
  val FAST_LIMIT = 100
}

trait CorrectnessTest extends Matchers with SparkTest with DatasetComparer {
  this: FlatSpec =>
  if (CorrectnessTest.FAST) {
    System.err.println("Running correctness test in fast mode")
  }

  private val queryCache = new QueryCache(Utils.getQueryCachePath, sp)

  def assertRDDEqual(a: RDD[Row], e: RDD[Row]): Unit = {
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

  def assertDataSetEqual(a: DataFrame, e: DataFrame) = {
    assertSmallDatasetEquality(a, e, ignoreNullable = true, orderedComparison = false)
  }

  def assertRDDSetEqual(a: RDD[Row], e: RDD[Row], setSize: Int) = {
    val aSet = a.map(r => r.toSeq.toSet)
    val eSet = e.map(r => r.toSeq.toSet)

    aSet.filter(_.size != setSize).isEmpty() should be(true)
    eSet.filter(_.size != setSize).isEmpty() should be(true)


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

    empty1 should be(true)
    empty2 should be(true)
  }

  private def getPathQueryDataset(ds: DataFrame): DataFrame = {
    // TODO remove once path queries are fast enough
    if (CorrectnessTest.FAST) {
      ds
    } else {
      ds.limit(1000)
    }
  }

  def sparkPathJoins(dataSetPath: String, rawDataset: DataFrame): Unit = {
    val cacheKey = if (CorrectnessTest.FAST) {
      CacheKey(dataSetPath, "", CorrectnessTest.FAST_LIMIT, -1)
    } else {
      CacheKey(dataSetPath, "", -1, -1)
    }
    // TODO rewrite to use cache

    val ds = if (CorrectnessTest.FAST) {
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

      assertDataSetEqual(a, e)

      a.isEmpty should be(false)
      e.isEmpty should be(false)
    }

    "three-paths" should "be the same" in {
      val pathQuerySet = getPathQueryDataset(ds)

      val e = pathBinaryJoins(3, pathQuerySet, nodeSet1, nodeSet2).cache()
      val a = pathPattern(3, pathQuerySet, nodeSet1, nodeSet2).cache()

      assertDataSetEqual(a, e)

      a.isEmpty should be(false)
      e.isEmpty should be(false)
    }

    "four-path" should "be the same" in {
      val pathQuerySet = getPathQueryDataset(ds)

      val e = pathBinaryJoins(4, pathQuerySet, nodeSet1, nodeSet2)
      val a = pathPattern(4, pathQuerySet, nodeSet1, nodeSet2)

      assertDataSetEqual(a, e)

      a.isEmpty should be(false)
      e.isEmpty should be(false)
    }
  }

  def sparkTriangleJoinsSimple(parallelism: Int, shouldMaterialize: Boolean, dataSetPath: String, rawDataset: DataFrame): Unit = {
    val cacheKey = if (CorrectnessTest.FAST) {
      CacheKey(dataSetPath, "", CorrectnessTest.FAST_LIMIT, -1)
    } else {
      CacheKey(dataSetPath, "", -1, -1)
    }

    val ds = if (CorrectnessTest.FAST) {
      rawDataset.limit(CorrectnessTest.FAST_LIMIT)
    } else {
      rawDataset
    }

    it should "compute the same triangles" in {
      val tempParallelism = wcojConfig.getParallelism
      val tempShouldMaterialize = wcojConfig.getShouldMaterialize
      wcojConfig.setShouldMaterialize(shouldMaterialize).setParallelism(parallelism)

      val e = queryCache.getOrCompute(cacheKey.copy(queryName = "clique", size = 3), cliqueBinaryJoins(3, sp, ds)).cache()
      val a = cliquePattern(3, ds).cache()


      try {
        assertDataSetEqual(a, e)
      } catch {
        case ex : Exception => throw ex
      } finally {
        wcojConfig.setShouldMaterialize(tempShouldMaterialize).setParallelism(tempParallelism)
      }
    }

  }

  def sparkTriangleJoins(dataSetPath: String, rawDataset: DataFrame): Unit = {
    val cacheKey = if (CorrectnessTest.FAST) {
      CacheKey(dataSetPath, "", CorrectnessTest.FAST_LIMIT, -1)
    } else {
      CacheKey(dataSetPath, "", -1, -1)
    }

    val ds = if (CorrectnessTest.FAST) {
      rawDataset.limit(CorrectnessTest.FAST_LIMIT)
    } else {
      rawDataset
    }

    it should "compute the same triangles" in {
      val e = queryCache.getOrCompute(cacheKey.copy(queryName = "clique", size = 3), cliqueBinaryJoins(3, sp, ds))
      val a = cliquePattern(3, ds)

      assertDataSetEqual(a, e)
    }


    it should "compute the same triangles independ of the variable ordering" in {
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

      assertDataSetEqual(otherReordered, normalVariableOrdering)
    }

    it should "compute the same triangles as spark for circular triangles" in {
      import sp.implicits._

      val circular = ds.findPattern(
        """
          |(a) - [] -> (b);
          |(b) - [] -> (c);
          |(c) - [] -> (a)
          |""".stripMargin, List("a", "b", "c")).cache()

      val duos = ds.as("R")
        .joinWith(ds.as("S"), $"R.dst" === $"S.src")
      val triangles = duos.joinWith(ds.as("T"),
        condition = $"_2.dst" === $"T.src" && $"_1.src" === $"T.dst").cache()

      val goldStandard = triangles.selectExpr("_2.dst AS a", "_1._1.dst AS b", "_2.src AS c")

      assertDataSetEqual(circular, goldStandard)
    }
  }

  def sparkCliqueJoins(dataSetPath: String, rawDataset: DataFrame): Unit = {
    val cacheKey = if (CorrectnessTest.FAST) {
      CacheKey(dataSetPath, "clique", CorrectnessTest.FAST_LIMIT, -1)
    } else {
      CacheKey(dataSetPath, "clique", -1, -1)
    }

    val ds = if (CorrectnessTest.FAST) {
      rawDataset.limit(CorrectnessTest.FAST_LIMIT)
    } else {
      rawDataset
    }

    "Four clique" should "be the same" in {
      val a = cliquePattern(4, ds)
      val e = queryCache.getOrCompute(cacheKey.copy(size = 4), cliqueBinaryJoins(4, sp, ds))

      assertDataSetEqual(a, e)
    }

    "5-clique query" should "be the same" in {
      val a = cliquePattern(5, ds)
      val e = queryCache.getOrCompute(cacheKey.copy(size = 5), cliqueBinaryJoins(5, sp, ds))

      assertDataSetEqual(a, e)
    }

    "6-clique query" should "be the same" in {
      val a = cliquePattern(6, ds)
      val e = queryCache.getOrCompute(cacheKey.copy(size = 6), cliqueBinaryJoins(6, sp, ds))

      assertDataSetEqual(a, e)
    }
  }

  def sparkCycleJoins(dataSetPath: String, rawDataset: DataFrame): Unit = {
    val cacheKey = if (CorrectnessTest.FAST) {
      CacheKey(dataSetPath, "cycle", CorrectnessTest.FAST_LIMIT, -1)
    } else {
      CacheKey(dataSetPath, "cycle", -1, -1)
    }


    val ds = if (CorrectnessTest.FAST) {
      rawDataset.limit(CorrectnessTest.FAST_LIMIT)
    } else {
      rawDataset
    }

    "4-cylce" should "be the same" in {
      val a = cyclePattern(4, ds)
      val e = queryCache.getOrCompute(cacheKey.copy(size = 4), cycleBinaryJoins(4, ds))

      assertDataSetEqual(a, e)
    }

    "5-cylce" should "be the same" in {
      val a = cyclePattern(5, ds)
      val e = queryCache.getOrCompute(cacheKey.copy(size = 5), cycleBinaryJoins(5, ds))

      assertRDDSetEqual(a.rdd, e.rdd, 5)
    }

  }

  def sparkOtherJoins(dataSetPath: String, rawDataset: DataFrame): Unit = {
    val cacheKey = if (CorrectnessTest.FAST) {
      CacheKey(dataSetPath, "cycle", CorrectnessTest.FAST_LIMIT, -1)
    } else {
      CacheKey(dataSetPath, "cycle", -1, -1)
    }

    val ds = if (CorrectnessTest.FAST) {
      rawDataset.limit(CorrectnessTest.FAST_LIMIT)
    } else {
      rawDataset
    }

    "Diamond query" should "be the same" in {
      val a = DiamondQuery().applyPatternQuery(ds)
      val e = queryCache.getOrCompute(cacheKey.copy(queryName = "diamond"), DiamondQuery().applyBinaryQuery(ds, sp))

      assertDataSetEqual(a, e)
    }

    "House query" should "be the same" in {
      val a = housePattern(ds)
      val e = queryCache.getOrCompute(cacheKey.copy(queryName = "house"), houseBinaryJoins(sp, ds))

      assertDataSetEqual(a, e)
    }

    "kite" should "be the same" in {
      val a = queryCache.getOrCompute(cacheKey.copy(queryName = "kite"), kiteBinary(sp, ds))
      val e = kitePattern(ds)

      assertDataSetEqual(a, e)
    }
  }
}   
