package leapfrogTriejoin

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random


// TODO Unit tests are mostly obsolete after the optimizations
class ArraySearchSpec extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {

  "For a existing key" should "return the position" in {
    var v = Array(1L)
    ArraySearch.find(v, 1, 0, 1) should equal(0)

    v = Array(1, 2)
    ArraySearch.find(v, 1, 0, 2) should equal(0)

    v = Array(1, 2)
    ArraySearch.find(v, 2, 0, 2) should equal(1)

    v = Array(1, 2, 3)
    ArraySearch.find(v, 3, 0, 3) should equal(2)
  }

  "For a existing key it" should "return the first position" in {
    var v = Array[Long](2, 2)
    ArraySearch.find(v, 2, 0, 2) should equal(0)

    v = Array(1, 2, 2, 3)
    ArraySearch.find(v, 2, 0, 4) should equal(1)
  }

  "For a none-existing key it" should "return the first position of the next bigger element" in {
    var v = Array(1L)
    ArraySearch.find(v, 2, 0, 1) should equal(1)

    v = Array(1, 3)
    ArraySearch.find(v, 2, 0, 2) should equal(1)

    v = Array(1, 3, 3)
    ArraySearch.find(v, 2, 0, 2) should equal(1)
  }

  "It" should "return the first position of an element in the list if it exists and is in range" in {
    import org.scalacheck.Gen

    val array = Gen.nonEmptyBuildableOf[Array[Long], Long](Gen.posNum[Long])

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

      val ret = ArraySearch.find(sorted, key, start, end)
      assert(ret == keyPosition, s"In ${sorted.mkString(", ")} key $key should be found at $keyPosition not $ret with start $start and end $end")
    }
  }

  "It" should "return the first higher index if an element is not in the list" in {
    import org.scalacheck.Gen

    val array = Gen.buildableOfN[Array[Long], Long](100, Gen.posNum[Long])

    forAll(array) { a =>
      whenever(!a.isEmpty) {
        val noneExistingKey = Random.shuffle((0 to 200).toList).filter(!a.contains(_)).head
        val sorted = a.sorted
        val firstHigherElement = sorted.dropWhile(_ < noneExistingKey).headOption
        val keyPosition = if (firstHigherElement.isDefined) {
          sorted.indexOf(firstHigherElement.get)
        } else {
          sorted.length
        }
        val start = if (keyPosition == 0) {
          0
        } else {
          Random.nextInt(keyPosition)
        }
        val end = Math.max(start + Random.nextInt(sorted.length - start) + 1, keyPosition)

        val ret = ArraySearch.find(sorted, noneExistingKey, start, end)
        assert(ret == keyPosition, s"In ${sorted.mkString(", ")} with none-existing $noneExistingKey return $keyPosition not $ret with start $start and end $end")
      }
    }
  }

  "Regression 1: it" should "never access the array at end" in {
    ArraySearch.find(Array(1, 2), 3, 0, 2) should be(2)
  }
}
