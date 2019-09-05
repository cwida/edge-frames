package correctnessTesting

import experiments.Datasets._
import experiments.GraphWCOJ
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import partitioning.{AllTuples, FirstVariablePartitioningWithWorkstealing}
import testing.Utils._

class Orkut extends FlatSpec with CorrectnessTest with BeforeAndAfterAll {
  val DATASET_PATH = getDatasetPath("orkut")
  val ds = loadAmazonDataset(DATASET_PATH, sp).cache()

  override def beforeAll(): Unit = {
    wcojConfig.setJoinAlgorithm(GraphWCOJ)
    wcojConfig.setPartitioning(FirstVariablePartitioningWithWorkstealing())
    wcojConfig.setShouldMaterialize(true)
    wcojConfig.setParallelism(8)
    wcojConfig.setWorkstealingBatchSize(10)
  }

  override def afterAll(): Unit = {
    wcojConfig.setShouldMaterialize(false)
    wcojConfig.setParallelism(1)
    wcojConfig.setJoinAlgorithm(experiments.WCOJ)
    wcojConfig.setPartitioning(AllTuples())
  }

  "with parallelsim 17" should behave like sparkTriangleJoinsSimple(17, shouldMaterialize = true, DATASET_PATH, ds)

  "WCOJ" should behave like sparkTriangleJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkCliqueJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkCycleJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkOtherJoins(DATASET_PATH, ds)
}
