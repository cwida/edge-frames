package correctnessTesting

import experiments.Datasets._
import experiments.{GraphWCOJ, WCOJ}
import org.apache.spark.sql.WCOJFunctions
import org.scalatest.BeforeAndAfterAll
import testing.Utils

class GoogleWebWCOJ extends CorrectnessTest with BeforeAndAfterAll {
  val DATASET_PATH = Utils.getDatasetPath("web-google")
  val ds = loadGoogleWebGraph(DATASET_PATH, sp).cache()

  override def beforeAll(): Unit = {
    wcojConfig.setJoinAlgorithm(WCOJ)
  }

  override def afterAll(): Unit = {
    wcojConfig.setJoinAlgorithm(experiments.WCOJ)
  }

  "WCOJ" should behave like sparkTriangleJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkCliqueJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkCycleJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkOtherJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkPathJoins(DATASET_PATH, ds)
}
