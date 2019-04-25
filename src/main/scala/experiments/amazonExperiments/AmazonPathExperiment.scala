package experiments.amazonExperiments

import experiments.GenericExperiment
import org.apache.spark.sql.{DataFrame, SparkSession}
import experiments.Datasets.loadAmazonDataset
import experiments.Queries._

object AmazonPathExperiment extends App with GenericExperiment {

  val (nodeSet1, nodeSet2) = pathQueryNodeSets(ds)
  nodeSet1.cache().count()
  nodeSet2.cache().count()

  run()

  override def loadDataset(sp: SparkSession) = loadAmazonDataset(sp).limit(1000)

  override def runWCOJ(sp: SparkSession, dataSet: DataFrame) = fourPathPattern(dataSet, nodeSet1, nodeSet2).count()

  override def runBinaryJoins(sp: SparkSession, dataSet: DataFrame) = fourPathBinaryJoins(dataSet, nodeSet1, nodeSet2).count()
}
