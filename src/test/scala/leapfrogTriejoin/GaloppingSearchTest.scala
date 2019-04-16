package leapfrogTriejoin

import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnVector
import org.scalatest.{FlatSpec, Matchers, WordSpec}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import scala.util.Random

class GaloppingSearchTest extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {
  def buildColumnVector(a: Array[Int]): ColumnVector = {
    val v = new OnHeapColumnVector(1000, IntegerType)
    for (i <- a) {
      v.appendInt(i)
    }
    v
  }

  "For a existing key" should "return the position" in {
    var v = buildColumnVector(Array(1))
    GaloppingSearch.find(v, 1, 0, 1) should equal(0)

    v = buildColumnVector(Array(1, 2))
    GaloppingSearch.find(v, 1, 0, 2) should equal(0)

    v = buildColumnVector(Array(1, 2))
    GaloppingSearch.find(v, 2, 0, 2) should equal(1)

    v = buildColumnVector(Array(1, 2, 3))
    GaloppingSearch.find(v, 3, 0, 3) should equal(2)
  }

  "For a existing key it" should "return the first position" in {
    var v = buildColumnVector(Array(2, 2))
    GaloppingSearch.find(v, 2, 0, 2) should equal(0)

    v = buildColumnVector(Array(1, 2, 2, 3))
    GaloppingSearch.find(v, 2, 0, 4) should equal(1)
  }

  "For a none-existing key it" should "return the first position of the next bigger element" in {
    var v = buildColumnVector(Array(1))
    GaloppingSearch.find(v, 2, 0, 1) should equal(1)

    v = buildColumnVector(Array(1, 3))
    GaloppingSearch.find(v, 2, 0, 2) should equal(1)

    v = buildColumnVector(Array(1, 3, 3))
    GaloppingSearch.find(v, 2, 0, 2) should equal(1)
  }

  "It" should "return the first position of an element in the list if it exists and is in range" in {
    import org.scalacheck.Gen

    val array = Gen.nonEmptyBuildableOf[Array[Int], Int](Gen.posNum[Int])

    forAll(array) { a =>
      val key = Random.shuffle(a.toList).head
      val sorted = a.sorted
      val keyPosition = sorted.indexOf(key)
      val start = if (keyPosition == 0) {
        0
      } else {
        Random.nextInt(keyPosition)
      }
      val end = Math.max(start + Random.nextInt(sorted.length - start) + 1, keyPosition)

      val ret = GaloppingSearch.find(buildColumnVector(sorted), key, start, end)
      assert(ret == keyPosition, s"In ${sorted.mkString(", ")} key $key should be found at $keyPosition not $ret with start $start and end $end")
    }
  }

  "It" should "return the first higher index if an element is not in the list" in {
    import org.scalacheck.Gen

    // TODO array not set once it finds the first
    val array = Gen.buildableOfN[Array[Int], Int](100, Gen.posNum[Int])

    forAll(array) { a =>
      whenever(!a.isEmpty) {
        val noneExistingKey = Random.shuffle((0 to 200).toList).filter(!a.contains(_)).head
        val sorted = a.sorted
        val firstHigherElement = sorted.dropWhile(_ < noneExistingKey).headOption
        val keyPosition = if (firstHigherElement.isDefined) sorted.indexOf(firstHigherElement.get) else sorted.length
        val start = if (keyPosition == 0) {
          0
        } else {
          Random.nextInt(keyPosition)
        }
        val end = Math.max(start + Random.nextInt(sorted.length - start) + 1, keyPosition)

        val ret = GaloppingSearch.find(buildColumnVector(sorted), noneExistingKey, start, end)
        assert(ret == keyPosition, s"In ${sorted.mkString(", ")} with none-existing $noneExistingKey return $keyPosition not $ret with start $start and end $end")
      }
    }
  }
}
