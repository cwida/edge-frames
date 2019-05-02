package experiments

import java.nio.file.{Files, Path, Paths}

import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

object Datasets {
  val schema = new StructType()
    .add("src", IntegerType)
    .add("dst", IntegerType)

  def loadAmazonDataset(dataSetPath: String, sp: SparkSession): DataFrame = {
    loadAndCacheAsParquet(dataSetPath, sp.read
      .format("csv")
      .option("delimiter", "\t")
      .option("comment", "#")
      .schema(schema),
      sp)
      .repartition(1)
  }

  def loadSNBDataset(sp: SparkSession, datasetPath: String): DataFrame = {
    loadAndCacheAsParquet(datasetPath,  sp.read
      .format("csv")
      .option("delimiter", "|")
      .option("inferScheme", true),
      sp)
      .withColumnRenamed("_c0", "src")
      .withColumnRenamed("_c1", "dst")
      .repartition(1)
      // TODO move repartion out of these methods
  }

  def loadLiveJournalDataset(sp: SparkSession, datasetPath: String): DataFrame = {
    loadAndCacheAsParquet(datasetPath,
      sp.read
        .format("csv")
        .option("delimiter", "\t")
        .option("comment", "#")
        .schema(schema),
      sp
    ).repartition(1)
  }

  def loadAndCacheAsParquet(datasetFilePath: String, csvReader: DataFrameReader, sp: SparkSession): DataFrame = {
    val parquetFile = datasetFilePath + ".parquet"
    if (Files.exists(Paths.get(parquetFile.replace("file://", "")))) {
      sp.read.parquet(parquetFile)
    } else {
      println("Parquet file not existing")
      val df = csvReader.csv(datasetFilePath + ".csv")
      println("Caching as parquet file")
      df.write.parquet(parquetFile)
      df
    }
  }

}
