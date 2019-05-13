package experiments

import java.nio.file.{Files, Path, Paths}

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SparkSession}

object Datasets {
  val schema = new StructType()
    .add("src", IntegerType)
    .add("dst", IntegerType)

  private def snapDatasetReader(sp: SparkSession): DataFrameReader = {
    sp.read
      .format("csv")
      .option("delimiter", "\t")
      .option("comment", "#")
      .schema(schema)
  }

  def loadAmazonDataset(dataSetPath: String, sp: SparkSession): DataFrame = {
    loadAndCacheAsParquet(dataSetPath,
      snapDatasetReader(sp),
      sp)
      .repartition(1)
  }

  def loadSNBDataset(sp: SparkSession, datasetPath: String): DataFrame = {
    loadAndCacheAsParquet(datasetPath, sp.read
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
      snapDatasetReader(sp),
      sp
    ).repartition(1)
  }

  def loadTwitterSnapEgo(sp: SparkSession, datasetPath: String): DataFrame = {
    import sp.implicits._
    val parquetFile = datasetPath + ".parquet"
    val d = if (Files.exists(Paths.get(parquetFile.replace("file://", "")))) {
      sp.read.parquet(parquetFile)
    } else {
      println("Parquet file not existing")
      val df =
        sp.read
          .format("csv")
          .option("delimiter", " ")
          .option("comment", "#")
          .schema(schema)
          .csv(datasetPath + ".csv")
          .toDF("src", "dst")
          .filter($"src" =!= $"dst")
          .distinct()
          .sort("src", "dst") // TODO so the others were presorted that of course changes my setup time quite a bit
      println("Caching as parquet file")
      df.write.parquet(parquetFile)
      df
    }
    d.repartition(1)
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
