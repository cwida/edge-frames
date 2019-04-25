package experiments.amazonExperiments

import java.util.Timer

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import sparkIntegration.WCOJ2WCOJExec
import sparkIntegration.implicits._

import scala.io.StdIn
import experiments.Datasets.loadAmazonDataset
import experiments.GenericExperiment
import experiments.Queries.{triangleBinaryJoins, trianglePattern}

object AmazonExperiment extends App with GenericExperiment {
  override def loadDataset(sp: SparkSession) = loadAmazonDataset(sp)

  override def runWCOJ(sp: SparkSession, dataSet: DataFrame) = triangleBinaryJoins(sp, dataSet).count()

  override def runBinaryJoins(sp: SparkSession, dataSet: DataFrame) = trianglePattern(dataSet).count()

  run()
}
