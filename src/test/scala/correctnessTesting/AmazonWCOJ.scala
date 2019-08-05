package correctnessTesting

import experiments.Datasets.loadAmazonDataset
import experiments.WCOJ
import org.apache.spark.sql.WCOJFunctions
import org.scalatest.BeforeAndAfterAll
import testing.Utils

class AmazonWCOJ extends CorrectnessTest with BeforeAndAfterAll {
  val DATASET_PATH = Utils.getDatasetPath("amazon-0302")
  val ds = loadAmazonDataset(DATASET_PATH, sp).cache()

  override def beforeAll(): Unit = {
    wcojConfig.setJoinAlgorithm(WCOJ)
  }

  "WCOJ" should behave like sparkTriangleJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkCliqueJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkCycleJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkOtherJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkPathJoins(DATASET_PATH, ds)
}
