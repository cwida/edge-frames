package correctnessTesting

import java.nio.file.{Files, Path, Paths}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class CacheKey(dataset: String, queryName: String, limit: Int, size: Int)  {
  def fileName: String = hashCode().toString
}

class QueryCache(cachePath: String, sp: SparkSession) {

  def getOrCompute(key: CacheKey, lazyQuery: DataFrame): DataFrame = {
    if (isCached(key)) {
      get(key, sp)
    } else {
      val ds = lazyQuery
      ds.write.parquet(Seq(cachePath, key.fileName + ".parquet").mkString("/"))
      ds
    }
  }

  def get(key: CacheKey, sp: SparkSession): DataFrame = {
    sp.read.parquet(Seq(cachePath, key.fileName + ".parquet").mkString("/"))
  }

  def isCached(key: CacheKey): Boolean =  {
    Files.exists(Paths.get(cachePath, key.fileName + ".parquet"))
  }


}
