package sparkIntegration

import leapfrogTriejoin.ArrayTrieIterable
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import sparkIntegration.implicits._
import testing.SparkTest

class ToEdgeRelationTest extends FlatSpec with Matchers with BeforeAndAfterAll with GeneratorDrivenPropertyChecks with SparkTest {

  "ToEdgeRelation" should "add edges in the other direction" in {
    import sp.implicits._
    val ds = sp.sparkContext.parallelize(Seq((1,2))).toDF("src", "dst").as[(Int, Int)]

    val edgeRel = ds.toEdgeRelationship()

    edgeRel.collect() should contain theSameElementsAs Seq((1, 2, true), (2, 1, false))
  }

  "ToEdgeRelation" should "not add existing edges as copy again" in {
    import sp.implicits._
    val ds = sp.sparkContext.parallelize(Seq((1,2), (2, 1))).toDF("src", "dst").as[(Int, Int)]

    val edgeRel = ds.toEdgeRelationship()

    edgeRel.collect() should contain theSameElementsAs Seq((1, 2, true), (2, 1, true))
  }

  "ToEdgeRelation" should "sort by src and dst" in {
    import sp.implicits._
    val ds = sp.sparkContext.parallelize(Seq((2,3), (2, 1), (1, 3))).toDF("src", "dst").as[(Int, Int)]

    val edgeRel = ds.toEdgeRelationship()

    edgeRel.collect() should contain theSameElementsInOrderAs Seq((1, 2, false), (1, 3, true), (2, 1, true), (2, 3, true), (3, 1, false), (3, 2, false))
  }


  "ToEdgeRelation" should "sort by src and dst, add edges in the opposite direction, not duplicate existing edges (property based)" in {
    import sp.implicits._

    val positiveIntTuples = Gen.buildableOf[Set[(Int, Int)], (Int, Int)](Gen.zip(Gen.posNum[Int], Gen.posNum[Int]))

    forAll (positiveIntTuples) { l =>
      whenever(l.forall(t => t._1 > 0 && t._2 > 0)) {  // Sad way to ensure numbers are actually positive
        val expectedOutput = l.toArray.flatMap(
          { case (src, dst) => {
            if (l.contains((dst, src))) {
              Seq((src, dst, true))
            } else {
              Seq((src, dst, true), (dst, src, false))
            }
          }}).sorted

        val ds: Dataset[(Int, Int)] = sp.sparkContext.parallelize(l.toSeq).toDF("src", "dst").as[(Int, Int)]

        val edgeRel = ds.toEdgeRelationship()

        edgeRel.collect should contain theSameElementsInOrderAs expectedOutput

      }
    }

  }
}
