package sparkIntegration

import experiments.Algorithm
import leapfrogTriejoin.MaterializingLeapfrogJoin
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import partitioning.{AllTuples, Partitioning}

import scala.collection.mutable

// TODO correct overwriting of getters and setters in Scala?
class WCOJConfiguration private (spark: SparkSession) {
  var broadcastTimeout : Int = spark.sqlContext.getConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key).toInt
  private var parallelism: Int = spark.sparkContext.getConf.get("spark.default.parallelism", "1").toInt
  private var joinAlgorithm: Algorithm = experiments.WCOJ
  private var partitioning: Partitioning = AllTuples()

  def getParallelism: Int = {
    parallelism
  }

  def setParallelism(p: Int): Unit = {
    parallelism = p
    println(s"Setting parallelism to $p")
  }

  def setJoinAlgorithm(a: Algorithm): Unit = {
    if (a == experiments.WCOJ) {
      MaterializingLeapfrogJoin.setShouldMaterialize(false)
    }
    joinAlgorithm = a
    println(s"Setting join algorithm to $a")
  }

  def getJoinAlgorithm: Algorithm = {
    joinAlgorithm
  }

  def setPartitioning(p: Partitioning): Unit = {
    partitioning = p
    println(s"Setting partitioning to $p")
  }

  def getPartitioning: Partitioning = {
    partitioning
  }

  def setShouldMaterialize(value: Boolean): Unit = {
    MaterializingLeapfrogJoin.setShouldMaterialize(value)
  }
}

object WCOJConfiguration {
  private[this] val configs = new mutable.HashMap[SparkContext, WCOJConfiguration]()

  def apply(spark: SparkSession): WCOJConfiguration = {
    if (configs.isDefinedAt(spark.sparkContext)) {
      throw new IllegalArgumentException(s"WCOJConfiguration for ${spark.sparkContext} already exists. Update existing configuration.")
    }

    val c = new WCOJConfiguration(spark)
    configs.put(spark.sparkContext, c)
    c
  }

  def get(sc:  SparkContext): WCOJConfiguration = {
    if (!configs.isDefinedAt(sc)) {
      throw new IllegalArgumentException(s"No WCOJ configuration initialized for $sc")
    }
    configs(sc)
  }
}
