// scalastyle:off println
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import sparkIntegration.WCOJ2WCOJExec
import sparkIntegration.implicits._

object LocalTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("Spark test")
      .set("spark.executor.memory", "2g")
      .set("spark.driver.memory", "2g")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    spark.experimental.extraStrategies = (Seq(WCOJ2WCOJExec) ++ spark.experimental.extraStrategies)

    val tuples1 = Array[(Int, Int)]((1, 2), (2, 5), (4, 2), (1, 5))
    val df: DataFrame = spark.sparkContext.parallelize(tuples1, 1).toDS()
      .withColumnRenamed("_1", "src")
      .withColumnRenamed("_2", "dst")

    val result = df.findPattern(
      """
        |(a) - [] -> (b);
        |(b) - [] -> (c);
        |(a) - [] -> (c)
        |""".stripMargin, List("a", "b", "c"))

    result.explain(true)
    val r = result.filter("a == 2")
    r.show()
    r.collect()
    result.show()

    result.collect()


    spark.stop()
  }
}
