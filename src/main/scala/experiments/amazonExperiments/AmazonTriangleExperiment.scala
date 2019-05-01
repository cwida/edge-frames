package experiments.amazonExperiments

import experiments.Datasets.loadAmazonDataset
import experiments.GenericExperiment
import experiments.Queries.{triangleBinaryJoins, trianglePattern}
import org.apache.spark.sql.{DataFrame, SparkSession}

object AmazonTriangleExperiment extends App with GenericExperiment {
  override def loadDataset(sp: SparkSession) = loadAmazonDataset("/home/per/workspace/master-thesis/datasets/amazon-0302.csv", sp)

  override def runWCOJ(sp: SparkSession, dataSet: DataFrame) =  trianglePattern(dataSet).count()

  override def runBinaryJoins(sp: SparkSession, dataSet: DataFrame) =triangleBinaryJoins(sp, dataSet).count()

  run()
}
