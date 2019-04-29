package experiments.amazonExperiments

import experiments.Datasets.loadAmazonDataset
import experiments.GenericExperiment
import experiments.Queries.{diamondPattern, diamondBinaryJoins}
import org.apache.spark.sql.{DataFrame, SparkSession}

object AmazonDiamondExperiment extends App with GenericExperiment {
  override def loadDataset(sp: SparkSession) = loadAmazonDataset(sp)

  override def runWCOJ(sp: SparkSession, dataSet: DataFrame) = diamondPattern(dataSet).count()

  override def runBinaryJoins(sp: SparkSession, dataSet: DataFrame) = diamondBinaryJoins(dataSet).count()

  run()
}
