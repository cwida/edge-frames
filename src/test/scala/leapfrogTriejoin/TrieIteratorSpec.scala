package leapfrogTriejoin

import org.scalatest.{FlatSpec, Matchers}

class TrieIteratorSpec extends FlatSpec with Matchers {

  "An empty TrieIterator" should "be at end" in {
    val iter = new TrieIterator(Array())
    assert(iter.atEnd)
  }

  "A TrieIterator" should "be linearly at end after reaching linear atEnd" in {
    val iter = new TrieIterator(Array((1, 1)))
    assert(!iter.atEnd)
    iter.open()
    iter.next()
    assert(iter.atEnd)
  }

  "A TrieIterator" should "be total at end after the last tuple" in {
    val iter = new TrieIterator(Array((1, 1)))
    assert(!iter.isAtTotalEnd)
    iter.open()
    iter.next()
    assert(iter.isAtTotalEnd)
  }

  "open" should "go a level down and to the first element" in {
    val iter = new TrieIterator(Array((1, 4), (1, 5), (2, 2)))

    iter.open()
    iter.key shouldBe 1
    iter.open()
    iter.key shouldBe 4
  }

  "open" should "be illegal after the last level" in {
    val iter = new TrieIterator(Array((1, 1)))
    iter.open()
    iter.open()
    assertThrows[IllegalStateException](iter.open)
  }

  "up" should "not change to the next element" in {
    val iter = new TrieIterator(Array((1, 1), (2, 2)))
    iter.open()
    iter.open()
    iter.up()
    iter.key shouldBe 1
    iter.open()
    iter.key shouldBe 1
  }

  "A TrieIterator" should "be linearly at end after the last tuple with certain value" in {
    val iter = new TrieIterator(Array((1, 2), (2, 3)))
    iter.open()
    iter.key shouldBe 1
    iter.open()
    iter.key shouldBe 2
    iter.next()
    assert(iter.atEnd)
  }

  "A TrieIterator" should "serve a level linearly and jump over values in lower levels" in {
    val iter = new TrieIterator(Array((1, 4), (1, 5), (2, 1)))
    iter.open()
    iter.key shouldBe 1
    iter.next()
    iter.key shouldBe 2
    iter.next()
    assert(iter.atEnd)
  }

  "After seek for none-existent argument, a iterator" should "be at the next element" in {
    val iter = new TrieIterator(Array((1, 1), (2, 3), (2, 4), (4, 0)))
    iter.open()
    iter.key shouldBe 1
    iter.seek(3)
    iter.key shouldBe 4
  }

}
