package leapfrogTriejoin

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import testing.Utils._

class CSRTrieIteratorSpec extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {

  // TODO add seek for value outside of range on level 0 and level 1
  // TODO add open to level -1 to test that src is reset
  "An empty testTrieIterator" should "be at end" in {
    val iter = CSRTrieIterable.buildBothDirectionsFrom(Array[(Long, Long)](), Array[(Long, Long)]())._1.trieIterator
    assert(iter.atEnd)
  }

  "A TrieIterator" should "be linearly at end after reaching linear atEnd" in {
    val iter = CSRTrieIterable.buildBothDirectionsFrom(Array[(Long, Long)]((1, 1)), Array[(Long, Long)]((1, 1)))._1.trieIterator
    assert(!iter.atEnd)
    iter.open()
    iter.next()
    assert(iter.atEnd)
  }

  "open" should "go a level down and to the first element" in {
    val iter = CSRTrieIterable.buildBothDirectionsFrom(Array[(Long, Long)]((1, 4), (1, 5), (2, 2)),
      Array[(Long, Long)]((2, 2), (4, 1), (5, 1)))._1.trieIterator

    iter.open()
    iter.translate(iter.key.toInt) shouldBe 1
    iter.open()
    iter.translate(iter.key.toInt) shouldBe 4
  }

  "up" should "not change to the next element" in {
    val iter = CSRTrieIterable.buildBothDirectionsFrom(Array[(Long, Long)]((1, 1), (2, 2)),
      Array[(Long, Long)]((1, 1), (2, 2)))._1
      .trieIterator
    iter.open()
    iter.open()
    iter.up()
    iter.translate(iter.key.toInt) shouldBe 1
    iter.open()
    iter.translate(iter.key.toInt) shouldBe 1
  }

  "A testTrieIterator" should "be linearly at end after the last tuple with certain value" in {
    val iter = CSRTrieIterable.buildBothDirectionsFrom(Array[(Long, Long)]((1, 2), (2, 3)),
      Array[(Long, Long)]((2, 1), (3, 2)))._1.trieIterator
    iter.open()
    iter.translate(iter.key.toInt) shouldBe 1
    iter.open()
    iter.translate(iter.key.toInt) shouldBe 2
    iter.next()
    assert(iter.atEnd)
  }

  "A TrieIterator" should "serve a level linearly and jump over values in lower levels" in {
    val iter = CSRTrieIterable.buildBothDirectionsFrom(Array[(Long, Long)]((1, 4), (1, 5), (2, 1)),
      Array[(Long, Long)]((1, 2), (4, 1), (5, 1)))._1.trieIterator
    iter.open()
    iter.translate(iter.key.toInt) shouldBe 1
    iter.next()
    iter.translate(iter.key.toInt) shouldBe 2
    iter.next()
    iter.atEnd should be (true)
  }

  "After seek for none-existent argument, a iterator" should "be at the next element" in {
    val iter = CSRTrieIterable.buildBothDirectionsFrom(Array[(Long, Long)]((1, 1), (2, 3), (2, 4), (4, 0)),
      Array[(Long, Long)]((0, 4), (1, 1), (3, 2), (4, 2)))._1.trieIterator
    iter.open()
    iter.translate(iter.key.toInt) shouldBe 1
    iter.seek(3)
    iter.translate(iter.key.toInt) shouldBe 4
  }

  "A testTrieIterator level that is reopened" should "start from the beginning again" in {
    val iter = CSRTrieIterable.buildBothDirectionsFrom(Array[(Long, Long)]((1, 2)),
      Array[(Long, Long)]((2, 1)))._1.trieIterator
    iter.open()
    iter.open()
    iter.translate(iter.key.toInt) shouldBe 2
    iter.seek(3)
    iter.atEnd shouldBe true
    iter.up()
    iter.atEnd shouldBe false
    iter.open()
    iter.translate(iter.key.toInt) shouldBe 2
  }

  "A TrieIterator traversal, without seeks" should "enumerate all values in order (specific)" in {
    val tuples1 = Array[(Long, Long)]((12,1), (13,1), (13,2), (16,1))
    val tuples2 = tuples1.map(t => (t._2, t._1)).sorted
    println(tuples2.mkString(", "))
    val iter = CSRTrieIterable.buildBothDirectionsFrom(tuples1, tuples2)._1.trieIterator

    traverseTrieIterator(iter).map(t => (iter.translate(t._1.toInt), iter.translate(t._2.toInt))) should contain theSameElementsInOrderAs
      tuples1
  }

  "A TrieIterator traversal, without seeks" should "enumerate all values in order" in {
    import org.scalacheck.Gen

    // Generates sets for uniqueness
    val positiveIntTuples = Gen.buildableOf[Set[(Long, Long)], (Long, Long)](Gen.zip(Gen.posNum[Long], Gen.posNum[Long]))

    forAll (positiveIntTuples) { l =>
      whenever(l.forall(t => t._1 > 0 && t._2 > 0)) {  // Sad way to ensure numbers are actually positive
        val tuples1 = l.toArray.sorted
        val tuples2 = tuples1.map(t => (t._2, t._1)).sorted
        val iter = CSRTrieIterable.buildBothDirectionsFrom(tuples1, tuples2)._1.trieIterator
        traverseTrieIterator(iter).map(t => (iter.translate(t._1.toInt), iter.translate(t._2.toInt))) should contain
        theSameElementsInOrderAs (tuples1)
      }
    }
  }


  "The second TrieIterator build" should "enumerate the same relationship in the opposite attribute order" in {
    import org.scalacheck.Gen

    // Generates sets for uniqueness
    val positiveIntTuples = Gen.buildableOf[Set[(Long, Long)], (Long, Long)](Gen.zip(Gen.posNum[Long], Gen.posNum[Long]))

    forAll (positiveIntTuples) { l =>
      whenever(l.forall(t => t._1 > 0 && t._2 > 0)) {  // Sad way to ensure numbers are actually positive
        val tuples1 = l.toArray.sorted
        val tuples2 = tuples1.map(t => (t._2, t._1)).sorted
        val (iterable1, iterable2) = CSRTrieIterable.buildBothDirectionsFrom(tuples1, tuples2)
        val (iterator1, iterator2) = (iterable1.trieIterator, iterable2.trieIterator)
        traverseTrieIterator(iterator2).map(t => (t._2, t._1)).sorted should contain theSameElementsInOrderAs (traverseTrieIterator
        (iterator1))
      }
    }
  }
}
