package leapfrogTriejoin

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import testing.Utils._

class ArrayTrieIteratorSpec extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {

  "An empty testTrieIterator" should "be at end" in {
    val iter = new ArrayTrieIterable(Array[(Int, Int)]()).trieIterator
    assert(iter.atEnd)
  }

  "A TrieIterator" should "be linearly at end after reaching linear atEnd" in {
    val iter = new ArrayTrieIterable(Array((1, 1))).trieIterator
    assert(!iter.atEnd)
    iter.open()
    iter.next()
    assert(iter.atEnd)
  }

  "open" should "go a level down and to the first element" in {
    val iter = new ArrayTrieIterable(Array((1, 4), (1, 5), (2, 2))).trieIterator

    iter.open()
    iter.key shouldBe 1
    iter.open()
    iter.key shouldBe 4
  }

  "up" should "not change to the next element" in {
    val iter = new ArrayTrieIterable(Array((1, 1), (2, 2))).trieIterator
    iter.open()
    iter.open()
    iter.up()
    iter.key shouldBe 1
    iter.open()
    iter.key shouldBe 1
  }

  "A testTrieIterator" should "be linearly at end after the last tuple with certain value" in {
    val iter = new ArrayTrieIterable(Array((1, 2), (2, 3))).trieIterator
    iter.open()
    iter.key shouldBe 1
    iter.open()
    iter.key shouldBe 2
    iter.next()
    assert(iter.atEnd)
  }

  "A testTrieIterator" should "serve a level linearly and jump over values in lower levels" in {
    val iter = new ArrayTrieIterable(Array((1, 4), (1, 5), (2, 1))).trieIterator
    iter.open()
    iter.key shouldBe 1
    iter.next()
    iter.key shouldBe 2
    iter.next()
    assert(iter.atEnd)
  }

  "After seek for none-existent argument, a iterator" should "be at the next element" in {
    val iter = new ArrayTrieIterable(Array((1, 1), (2, 3), (2, 4), (4, 0))).trieIterator
    iter.open()
    iter.key shouldBe 1
    iter.seek(3)
    iter.key shouldBe 4
  }

  "A testTrieIterator level that is reopened" should "start from the beginning again" in {
    val iter = new ArrayTrieIterable(Array((1, 2))).trieIterator
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

  "test" should "be at the next element" in {
    val tuples = Array((12,1), (13,1), (13,2), (16,1))
    val iter = new ArrayTrieIterable(tuples).trieIterator

    traverseTrieIterator(iter) should contain theSameElementsInOrderAs tuples
  }

  "A testTrieIterator traversal, without seeks," should "enumerate all values in order" in {
    import org.scalacheck.Gen

    // Generates sets for uniqueness
    val positiveIntTuples = Gen.buildableOf[Set[(Int, Int)], (Int, Int)](Gen.zip(Gen.posNum[Int], Gen.posNum[Int]))

    forAll (positiveIntTuples) { l =>
      whenever(l.forall(t => t._1 > 0 && t._2 > 0)) {  // Sad way to ensure numbers are actually positive
        val array = l.toArray.sorted
        val iter = new ArrayTrieIterable(array).trieIterator
        traverseTrieIterator(iter) should contain theSameElementsInOrderAs (array)
      }
    }
  }



}
