package leapfrogTriejoin

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import testing.Utils.traverseTrieIterator

class TreeTrieIteratorSpec extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {

  "An empty TrieIterator" should "be at end" in {
    val iter = new TreeTrieIterator(Array[(Long, Long)]())
    assert(iter.atEnd)
  }

  "A TrieIterator" should "be linearly at end after reaching linear atEnd" in {
    val iter = new TreeTrieIterator(Array[(Long, Long)]((1, 1)))
    assert(!iter.atEnd)
    iter.open()
    iter.next()
    assert(iter.atEnd)
  }

  "A TrieIterator" should "be total at end after the last tuple" in {
    val iter = new TreeTrieIterator(Array[(Long, Long)]((1, 1)))
    assert(!iter.isAtTotalEnd)
    iter.open()
    iter.next()
    assert(iter.isAtTotalEnd)
  }

  "open" should "go a level down and to the first element" in {
    val iter = new TreeTrieIterator(Array[(Long, Long)]((1, 4), (1, 5), (2, 2)))

    iter.open()
    iter.key shouldBe 1
    iter.open()
    iter.key shouldBe 4
  }

  "open" should "be illegal after the last level" in {
    val iter = new TreeTrieIterator(Array[(Long, Long)]((1, 1)))
    iter.open()
    iter.open()
    assertThrows[IllegalStateException](iter.open)
  }

  "up" should "not change to the next element" in {
    val iter = new TreeTrieIterator(Array[(Long, Long)]((1, 1), (2, 2)))
    iter.open()
    iter.open()
    iter.up()
    iter.key shouldBe 1
    iter.open()
    iter.key shouldBe 1
  }

  "A TrieIterator" should "be linearly at end after the last tuple with certain value" in {
    val iter = new TreeTrieIterator(Array[(Long, Long)]((1, 2), (2, 3)))
    iter.open()
    iter.key shouldBe 1
    iter.open()
    iter.key shouldBe 2
    iter.next()
    assert(iter.atEnd)
  }

  "A TrieIterator" should "serve a level linearly and jump over values in lower levels" in {
    val iter = new TreeTrieIterator(Array[(Long, Long)]((1, 4), (1, 5), (2, 1)))
    iter.open()
    iter.key shouldBe 1
    iter.next()
    iter.key shouldBe 2
    iter.next()
    assert(iter.atEnd)
  }

  "After seek for none-existent argument, a iterator" should "be at the next element" in {
    val iter = new TreeTrieIterator(Array[(Long, Long)]((1, 1), (2, 3), (2, 4), (4, 0)))
    iter.open()
    iter.key shouldBe 1
    iter.seek(3)
    iter.key shouldBe 4
  }

  "A TrieIterator level that is reopened" should "start from the beginning again" in {
    val iter = new TreeTrieIterator(Array[(Long, Long)]((1, 2)))
    iter.open()
    iter.open()
    iter.key shouldBe 2
    iter.seek(3)
    iter.atEnd shouldBe true
    iter.up()
    iter.atEnd shouldBe false
    iter.open()
    iter.key shouldBe 2
  }

  "A TrieIterator traversal, without seeks," should "enumerate all values in order" in {
    import org.scalacheck.Gen

    // Generates sets for uniqueness
    val positiveIntTuples = Gen.buildableOf[Set[(Long, Long)], (Long, Long)](Gen.zip(Gen.posNum[Long], Gen.posNum[Long]))

    forAll (positiveIntTuples) { l =>
      whenever(l.forall(t => t._1 >0 && t._2 > 0)) {  // Sad way to ensure numbers are actually positive
        val array = l.toArray.sorted
        val iter = new TreeTrieIterator(array)
        traverseTrieIterator(iter) should contain theSameElementsInOrderAs (array)
      }
    }
  }



}
