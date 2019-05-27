package correctnessTesting

import experiments.Datasets.loadAmazonDataset
import experiments.GraphWCOJ
import org.apache.spark.sql.WCOJFunctions
import org.scalatest.BeforeAndAfterAll

class AmazonGraphWCOJ extends CorrectnessTest with BeforeAndAfterAll {
  val DATASET_PATH = "/home/per/workspace/master-thesis/datasets/amazon-0302"
  val ds = loadAmazonDataset(DATASET_PATH, sp).cache()

  override def beforeAll(): Unit = {
    WCOJFunctions.setJoinAlgorithm(GraphWCOJ)
  }

  "WCOJ" should behave like sparkJoins(ds)
}
