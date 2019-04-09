// scalastyle:off println
import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import scalaIntegration.WCOJ2WCOJExec
import scalaIntegration.implicits._

import scala.io.StdIn.readLine

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

    val tuples1 = Array[(Int, Int)]((1, 2), (3, 3), (4, 2), (5, 1))
    val ds : Dataset[(Int, Int)] = spark.sparkContext.parallelize(tuples1, 1).toDS()

    val result = ds.findPattern(
      """
        |(a) - [] -> (b);
        |(b) - [] -> (c);
        |(a) - [] -> (c)
        |""".stripMargin, List("a", "b", "c"))

    result.show()

    spark.stop()
  }
}
