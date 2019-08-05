package correctnessTesting

import java.net.InetAddress

import experiments.Datasets.loadAmazonDataset
import experiments.GraphWCOJ
import org.apache.spark.sql.{DataFrame, WCOJFunctions}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import partitioning.{AllTuples, Shares}
import testing.Utils._

class SharesPartitioning extends FlatSpec with CorrectnessTest with BeforeAndAfterAll {
  val DATASET_PATH = getDatasetPath("amazon-0302")
  val ds = loadAmazonDataset(DATASET_PATH, sp).cache()

  override def beforeAll(): Unit = {
    wcojConfig.setJoinAlgorithm(GraphWCOJ)
    wcojConfig.setPartitioning(Shares())
    wcojConfig.setShouldMaterialize(true)
    wcojConfig.setParallelism(8)
  }

  override def afterAll(): Unit = {
    wcojConfig.setShouldMaterialize(false)
    wcojConfig.setParallelism(1)
    wcojConfig.setJoinAlgorithm(experiments.WCOJ)
    wcojConfig.setPartitioning(AllTuples())
  }

  "with parallelsim 17" should behave like sparkTriangleJoinsSimple(17, shouldMaterialize = true, DATASET_PATH, ds)
  "without materialization" should behave like sparkTriangleJoinsSimple(8, shouldMaterialize = false, DATASET_PATH, ds)
  "WCOJ" should behave like sparkTriangleJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkCliqueJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkCycleJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkOtherJoins(DATASET_PATH, ds)
}
