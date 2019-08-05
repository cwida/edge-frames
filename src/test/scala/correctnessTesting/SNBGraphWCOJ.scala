package correctnessTesting

import experiments.Datasets.loadAmazonDataset
import experiments.{GraphWCOJ, WCOJ}
import org.apache.spark.sql.WCOJFunctions
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import testing.Utils

class SNBGraphWCOJ extends FlatSpec with CorrectnessTest with BeforeAndAfterAll {
  val DATASET_PATH = Utils.getDatasetPath("snb1")
  val ds = loadAmazonDataset(DATASET_PATH, sp).cache()

  override def beforeAll(): Unit = {
    wcojConfig.setJoinAlgorithm(GraphWCOJ)
  }

  override def afterAll(): Unit = {
    wcojConfig.setJoinAlgorithm(experiments.WCOJ)
  }

  "WCOJ" should behave like sparkTriangleJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkCliqueJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkCycleJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkOtherJoins(DATASET_PATH, ds)
//  "WCOJ" should behave like sparkPathJoins(ds)
}
