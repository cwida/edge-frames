package partitioning

import leapfrogTriejoin.{CSRTrieIterable, TrieIterator}
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import math.Ordering
import scala.collection.mutable

class RangeFilteredTrieIteratorSpec extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks with BeforeAndAfterEach {

  val LOWER = 0
  val UPPER = 20

  def setup(ranges1: Array[(Int, Int)], ranges2: Array[(Int, Int)] = Array((LOWER, UPPER))): RangeFilteredTrieIterator = {
    // Graph is a line from 0 to 19. Additionally, 0 is connected to all vertices
    val edges: Array[(Long, Long)] =
      (Array((LOWER until UPPER).zip((LOWER + 1) until (UPPER + 1)): _*)
        ++ (LOWER + 2 until UPPER + 1).map(i => (0, i)))
        .sorted.map(e => (e._1.toLong, e._2.toLong))


    val trieIterables = CSRTrieIterable.buildBothDirectionsFrom(edges, edges.map(e => (e._2, e._1)).sorted)

    val ti = new RangeFilteredTrieIterator(0,
      ranges1.flatMap(r => Seq(r._1, r._2)),
      ranges2.flatMap(r => Seq(r._1, r._2)),
      trieIterables._1.trieIterator)
    ti.open()
    ti
  }

  def setupLevel1(ranges2: Array[(Int, Int)]): RangeFilteredTrieIterator = {
    val ti = setup(Array((LOWER, UPPER)), ranges2)
    ti.open()
    ti
  }

  def trieIteratorToSeq(ti: TrieIterator): Seq[Long] = {
    val buffer = mutable.Buffer[Long]()
    while (!ti.atEnd) {
      buffer.append(ti.key)
      ti.next()
    }
    buffer
  }


  "A RangeFilteredTrieIterator with a single covering range" should "not filter anything (level 0)" in {
    val ti = setup(Array((LOWER, UPPER)))

    trieIteratorToSeq(ti) should contain theSameElementsInOrderAs (LOWER until UPPER)
  }

  "A RangeFilteredTrieIterator with a single covering range" should "not filter anything (level 1)" in {
    val ti = setupLevel1(Array((LOWER + 1, UPPER + 1)))

    trieIteratorToSeq(ti) should contain theSameElementsInOrderAs (LOWER + 1 until UPPER + 1)
  }

  "A RangeFilteredTrieIterator with a single range" should "return only this range (level 0)" in {
    val ti = setup(Array((10, 15)))

    trieIteratorToSeq(ti) should contain theSameElementsInOrderAs (10 until 15)
  }

  "A RangeFilteredTrieIterator with a single range" should "return only this range (level 1)" in {
    val ti = setupLevel1(Array((10, 15)))

    trieIteratorToSeq(ti) should contain theSameElementsInOrderAs (10 until 15)
  }

  "A RangeFilteredTrieIterator with a single range of length 1" should "has only one element(level 0)" in {
    val ti = setup(Array((10, 11)))

    trieIteratorToSeq(ti) should contain theSameElementsInOrderAs Seq(10)
  }

  "A RangeFilteredTrieIterator with a single range of length 1" should "has only one element(level 1)" in {
    val ti = setupLevel1(Array((10, 11)))

    trieIteratorToSeq(ti) should contain theSameElementsInOrderAs Seq(10)
  }

  "A RangeFilteredTrieIterator with a two ranges" should "return only these two ranges (level 0)" in {
    val ti = setup(Array((10, 11), (14, 19)))

    trieIteratorToSeq(ti) should contain theSameElementsInOrderAs Seq(10) ++ Seq(14 until 19: _*)
  }

  "A RangeFilteredTrieIterator with a two ranges" should "return only these two ranges (level 1)" in {
    val ti = setupLevel1(Array((10, 11), (14, 19)))

    trieIteratorToSeq(ti) should contain theSameElementsInOrderAs Seq(10) ++ Seq(14 until 19: _*)
  }


  "A RangeFilteredTrieIterator with a three ranges" should "return only these three ranges (level 0)" in {
    val ti = setup(Array((0, 5), (10, 11), (14, UPPER)))

    trieIteratorToSeq(ti) should contain theSameElementsInOrderAs Seq(0 until 5: _*) ++ Seq(10) ++ Seq(14 until UPPER: _*)
  }

  "A RangeFilteredTrieIterator with a three ranges" should "return only these three ranges (level 1)" in {
    val ti = setupLevel1(Array((1, 4), (10, 11), (14, UPPER + 1)))

    trieIteratorToSeq(ti) should contain theSameElementsInOrderAs Seq(1 until 4: _*) ++ Seq(10) ++ Seq(14 until UPPER + 1: _*)
  }


  "A RangeFilteredTrieIterator" should "be seekable (level 0)" in {
    val ti = setup(Array((1, 5), (10, 11), (14, UPPER)))

    ti.seek(1)
    ti.key shouldBe 1L

    ti.seek(10)
    ti.key shouldBe 10

    ti.seek(11) // Not there
    ti.key shouldBe 14

    ti.seek(UPPER + 5)
    ti.atEnd shouldBe true

  }

  "A RangeFilteredTrieIterator" should "be seekable (level 1)" in {
    val ti = setup(Array((1, 5), (10, 11), (14, UPPER)))

    ti.seek(1)
    ti.key shouldBe 1L

    ti.seek(10)
    ti.key shouldBe 10

    ti.seek(11) // Not there
    ti.key shouldBe 14

    ti.seek(UPPER + 5)
    ti.atEnd shouldBe true
  }

  def upperBound(key: Int, seq: Seq[Int]): Int = {
    var i = 0
    while (i < seq.length && seq(i) < key) {
      i += 1
    }
    if (i == seq.length) {
      -1
    } else {
      seq(i)
    }
  }

  "A RangeFilteredTrieIterator with arbritrary four ranges" should "be seekable" in {
    val keysToSearch = 4

    val rangesGen = Gen.buildableOfN[Set[Int], Int](8, Gen.chooseNum(LOWER, UPPER))
    val keysGen = Gen.buildableOfN[Set[Int], Int](keysToSearch, Gen.chooseNum(LOWER, UPPER))

    forAll(rangesGen, keysGen) { (rs: Set[Int], ks) => {
      whenever(rs.nonEmpty
        && LOWER <= rs.min
        && ks.nonEmpty
        && ks.max < UPPER
        && LOWER <= ks.min
      ) {
        val evenRS = if (rs.size % 2 == 0) rs else rs ++ Seq(UPPER + 5)
        val ranges = evenRS.toSeq.sorted.grouped(2).map(s => (s.head, s(1))).toArray
//        println(ranges.mkString(", "))
        val keys = (ks ++ Seq(7, 8, 15)).take(4).toSeq.sorted

        val allAvailableKeys = ranges.flatMap(r => r._1 until r._2).toSeq.filter(_ < UPPER)
        val keySearchResults = keys.map(k => upperBound(k, allAvailableKeys))

        val ti = setup(ranges)

        if (ti.key < keys(0)) {
          ti.seek(keys(0))
          ti.atEnd shouldBe keySearchResults(0) == -1
          assert(ti.key == keySearchResults(0) || keySearchResults(0) == -1)
        }

        if (!ti.atEnd && ti.key < keys(1)) {
          ti.seek(keys(1))
          ti.atEnd shouldBe keySearchResults(1) == -1
          assert(ti.key == keySearchResults(1) || keySearchResults(1) == -1)
        }

        if (!ti.atEnd && ti.key < keys(2)) {
          ti.seek(keys(2))
          ti.atEnd shouldBe keySearchResults(2) == -1
          assert(ti.key == keySearchResults(2) || keySearchResults(2) == -1)
        }

        if (!ti.atEnd && ti.key < keys(3)) {
          ti.seek(keys(3))
          ti.atEnd shouldBe keySearchResults(3) == -1
          assert(ti.key == keySearchResults(3) || keySearchResults(3) == -1)
        }
      }
    }
    }
  }

}
