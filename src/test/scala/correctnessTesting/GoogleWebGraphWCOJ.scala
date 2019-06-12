package correctnessTesting

import experiments.Datasets._
import experiments.GraphWCOJ
import org.apache.spark.sql.WCOJFunctions
import org.scalatest.BeforeAndAfterAll

class GoogleWebGraphWCOJ extends CorrectnessTest with BeforeAndAfterAll {
  val DATASET_PATH = "/home/per/workspace/master-thesis/datasets/web-google"
  val ds = loadGoogleWebGraph(DATASET_PATH, sp).cache()

  override def beforeAll(): Unit = {
    WCOJFunctions.setJoinAlgorithm(GraphWCOJ)
  }

  override def afterAll(): Unit = {
    WCOJFunctions.setJoinAlgorithm(experiments.WCOJ)
  }

  "WCOJ" should behave like sparkTriangleJoins(ds)
  "WCOJ" should behave like sparkCliqueJoins(ds)
  "WCOJ" should behave like sparkCycleJoins(ds)
  "WCOJ" should behave like sparkOtherJoins(ds)
//  "WCOJ" should behave like sparkPathJoins(ds)  \\ TODO fix path joins
}
