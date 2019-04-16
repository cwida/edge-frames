package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.OrderedRDDFunctions
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import sparkIntegration.implicits._
import sparkIntegration.{ToTrieIterableRDDExec, WCOJ2WCOJExec, WCOJExec}

class WCOJSparkIntegration extends FlatSpec with Matchers with BeforeAndAfterAll {
  val conf = new SparkConf()
    .setMaster("local[1]")
    .setAppName("Spark test")
    .set("spark.executor.memory", "2g")
    .set("spark.driver.memory", "2g")

  val spark = SparkSession.builder().config(conf).getOrCreate()


  spark.experimental.extraStrategies = (Seq(WCOJ2WCOJExec) ++ spark.experimental.extraStrategies)


  val sp = spark
  import sp.implicits._

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


  "Logical and physical plan" should "reference src and dest from all children" in {
    val logicalPlan = result.logicalPlan
    val physicalPlan = result.queryExecution.sparkPlan

    logicalPlan.references.map(_.name) should contain theSameElementsAs List("src", "src", "src", "dst", "dst", "dst")
    physicalPlan.references.map(_.name) should contain theSameElementsAs List("src", "src", "src", "dst", "dst", "dst")
  }

  "Logical and physical plan" should "output the attributes as defined in the pattern" in {
    val logicalPlan = result.logicalPlan
    val physicalPlan = result.queryExecution.sparkPlan

    logicalPlan.output.map(_.name) should contain theSameElementsAs List("a", "b", "c")
    physicalPlan.output.map(_.name) should contain theSameElementsAs List("a", "b", "c")
  }

  "Logical and physical plan" should "output should be different attributes than input attributes" in {
    val logicalPlan = result.logicalPlan
    val physicalPlan = result.queryExecution.sparkPlan

    logicalPlan.output.map(_.exprId).toList should have length logicalPlan.output.map(_.exprId).toSet.size
    physicalPlan.output.map(_.exprId).toList should have length physicalPlan.output.map(_.exprId).toSet.size
  }

  "Execution" should "triangle 1, 2, 5 in the input data" in {
    result.collect().map(_.toSeq) should contain only Seq(1, 2, 5)
  }

  "WCOJExec" should "be preceded by an ToTrieIterableRDDExec" in {
    val physicalPlan = result.queryExecution.sparkPlan

    val firstChildOfWCOJ = physicalPlan.collect({case WCOJExec(_, c :: _) => c }).head
    assert(firstChildOfWCOJ.isInstanceOf[ToTrieIterableRDDExec])
  }

}