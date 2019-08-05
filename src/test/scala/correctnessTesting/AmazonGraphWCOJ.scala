package correctnessTesting

import java.net.InetAddress

import experiments.Datasets.loadAmazonDataset
import experiments.GraphWCOJ
import org.apache.spark.sql.WCOJFunctions
import org.scalatest.BeforeAndAfterAll
import testing.Utils._

class AmazonGraphWCOJ extends CorrectnessTest with BeforeAndAfterAll {
  val DATASET_PATH = getDatasetPath("amazon-0302")
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
//  "WCOJ" should behave like sparkPathJoins(ds)  \\ TODO fix path joins
}
