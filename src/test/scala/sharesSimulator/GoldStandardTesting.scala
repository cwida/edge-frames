package sharesSimulator

import java.io.File
import java.nio.file.{Files, Paths}

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import experiments.{AmazonCoPurchase, Clique}
import org.apache.spark.sql.{DataFrame, types}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testing.SparkTest

class GoldStandardTesting extends FlatSpec with SparkTest with Matchers with DatasetComparer with BeforeAndAfterAll {
  val GOLDSTANDARD_DIR = "./shares-gold-standards/"
  val AMAZON_TRIANGLE_GOLD_STANDARD: String = GOLDSTANDARD_DIR + "ama-triangle"
  val AMAZON_5CLIQUE_GOLD_STANDARD: String = GOLDSTANDARD_DIR + "ama-5clique"
  val DATASET_PATH = "/home/per/workspace/master-thesis/datasets/amazon-0302"

  val overwriteGoldstandards = true

  val ds = AmazonCoPurchase.loadDataset(DATASET_PATH, sp).limit(10000).cache()
  val count = ds.count() // Trigger caching

  override def beforeAll(): Unit = {
    if (overwriteGoldstandards) {
      if (Files.exists(Paths.get(GOLDSTANDARD_DIR))) {
        testing.Utils.deleteRecursively(new File(GOLDSTANDARD_DIR))
      }

      val h = new Hypercube(64, Clique(3))
      var workerCounts = calculateWorkerCounts(h, ds)
      writeStandard(AMAZON_TRIANGLE_GOLD_STANDARD, workerCounts)

      val h5 = new Hypercube(64, Clique(5))
      workerCounts = calculateWorkerCounts(h5, ds)
      writeStandard(AMAZON_5CLIQUE_GOLD_STANDARD, workerCounts)
    }
  }

  "Amazon triangle, current implementation" should "equal the gold standard" in {
    val standard = readStandard(AMAZON_TRIANGLE_GOLD_STANDARD)
    val actual = calculateWorkerCounts(new Hypercube(64, Clique(3)), ds)
    assertSmallDatasetEquality(actual, standard, ignoreNullable = true)
  }

  "Amazon 5-clique, current implementation" should "equal the gold standard" in {
    val standard = readStandard(AMAZON_5CLIQUE_GOLD_STANDARD)
    val actual = calculateWorkerCounts(new Hypercube(64, Clique(5)), ds)
    assertSmallDatasetEquality(actual, standard, ignoreNullable = true)
  }

  private def calculateWorkerCounts(hypercube: Hypercube, ds: DataFrame): DataFrame = {
    import sp.implicits._
    val sharesPartitioning = hypercube.calculateWorkers(ds)
    sharesPartitioning
      .toDF("worker", "edge")
      .groupBy("worker")
      .count()
      .sort("worker")
  }

  private def readStandard(path: String): DataFrame = {
    val schema = new StructType()
        .add("worker", IntegerType, false)
        .add("count", LongType, false)
    sp.read.schema(schema).csv(path)
  }

  private def writeStandard(path: String, ds: DataFrame): Unit = {
    ds.coalesce(1).write.csv(path)
  }


}
