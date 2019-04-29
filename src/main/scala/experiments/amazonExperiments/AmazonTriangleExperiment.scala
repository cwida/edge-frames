package experiments.amazonExperiments

import experiments.Datasets.loadAmazonDataset
import experiments.GenericExperiment
import experiments.Queries.{triangleBinaryJoins, trianglePattern}
import org.apache.spark.sql.{DataFrame, SparkSession}

object AmazonTriangleExperiment extends App with GenericExperiment {
  override def loadDataset(sp: SparkSession) = loadAmazonDataset(sp)

  // TODO correct, exchange runWCOJ and runBinaryJoins implmentation
  override def runWCOJ(sp: SparkSession, dataSet: DataFrame) = triangleBinaryJoins(sp, dataSet).count()

  override def runBinaryJoins(sp: SparkSession, dataSet: DataFrame) = trianglePattern(dataSet).count()

  run()
}
