package sparkIntegration

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable

class WCOJConfiguration private (spark: SparkSession) {
  var broadcastTimeout : Int = spark.sqlContext.getConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key).toInt
  var parallelism: Int = spark.sparkContext.getConf.get("spark.default.parallelism").toInt
  // TODO add materializing in here
  // TODO add algorithm choice in here

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
    configs.get(sc).head
  }
}
