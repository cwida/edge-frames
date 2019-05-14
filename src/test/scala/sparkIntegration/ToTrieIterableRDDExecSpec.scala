package sparkIntegration

import leapfrogTriejoin.TrieIterable
import org.scalatest.{FlatSpec, Matchers}
import sparkIntegration.implicits._
import testing.SparkTest
import testing.Utils._

class ToTrieIterableRDDExecSpec extends FlatSpec with Matchers with SparkTest {
  import sp.implicits._

  val tuples1 = Array[(Int, Int)]((1, 2), (2, 5), (4, 2), (1, 5))
  val ds = sp.sparkContext.parallelize(tuples1, 1).toDS()
    .withColumnRenamed("_1", "src")
    .withColumnRenamed("_2", "dst")
    .as[(Int, Int)]


  "When attribute order from dst to src, it" should "produce a TrieIterator starting on dst" in {
    val trieIterable = ds.toTrieIterableRDD(Seq("dst", "src"))
    val physicalPlan = trieIterable.queryExecution.executedPlan
    val trieIterableExec = physicalPlan.collect({case t @  ToTrieIterableRDDExec(_, _) => t }).head

    val output = trieIterableExec.execute().asInstanceOf[TrieIterableRDD[TrieIterable]].trieIterables.map(ti => {
      traverseTrieIterator(ti.trieIterator)
    }).collect().flatten
    output should contain theSameElementsInOrderAs Seq((2, 1), (2, 4), (5, 1), (5, 2))
  }



}
