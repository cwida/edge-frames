package leapfrogTriejoin

import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

class MaterializingLeapfrogJoinSpec extends FlatSpec with GeneratorDrivenPropertyChecks with Matchers {

  def assertJoinEqual(join: MaterializingLeapfrogJoin, values: Seq[Long], translator: TrieIterator) = {
    val buffer = mutable.Buffer[Long]()
    while (!join.atEnd) {
      buffer.append(translator.translate(Array(join.key)).head)
      join.leapfrogNext()
    }
    buffer should contain theSameElementsInOrderAs values
  }

 def csrTrieIterator(tuples: Array[(Long, Long)]): TrieIterator = {
   CSRTrieIterable.buildBothDirectionsFrom(tuples, tuples.map(t => (t._2, t._1)).sorted)._1.trieIterator
 }

  "A empty join " should "throw an error on creation" in {
    assertThrows[IllegalArgumentException](new MaterializingLeapfrogJoin(Array[TrieIterator]()))
  }

  "A join over an empty relationship" should "at end" in {
    val join = new MaterializingLeapfrogJoin(Array(csrTrieIterator(Array[(Long, Long)]())))
    join.init()
    assert(join.atEnd)
  }

  "A join over a single relationship" should "equal the relationship for level 0" in {
    val tuples = Array((1L, 2L), (1L, 3L), (2L, 0L))
    val ti = csrTrieIterator(tuples)

    val join = new MaterializingLeapfrogJoin(Array(ti))
    ti.open()
    join.init()
    assertJoinEqual(join, Array(1L, 2L), ti)
  }

  "A join over a single relationship" should "equal the relationship for level 1" in {
    val tuples = Array((1L, 2L), (1L, 3L), (2L, 0L))
    val ti = csrTrieIterator(tuples)

    val join = new MaterializingLeapfrogJoin(Array(ti))
    ti.open()
    ti.open()
    join.init()
    assertJoinEqual(join, Array(2L, 3L), ti)
  }

  "A join over two relationship" should "be the intersection for both level 1" in {
    val tuples = Array((1L, 2L), (1L, 3L), (2L, 0L), (2L, 3L))

    val ti1 = csrTrieIterator(tuples)
    val ti2 = csrTrieIterator(tuples)

    val join = new MaterializingLeapfrogJoin(Array(ti1, ti2))
    ti1.open()
    ti1.open()

    ti2.open()
    ti2.next()
    ti2.open()

    join.init()
    assertJoinEqual(join, Seq(3L), ti1)
  }

  "A join over three relationship" should "be the intersection for both level 1" in {
    val tuples = Array((1L, 2L), (1L, 3L), (2L, 0L), (2L, 3L))

    val ti1 = csrTrieIterator(tuples)
    val ti2 = csrTrieIterator(tuples)
    val ti3 = csrTrieIterator(tuples)

    val join = new MaterializingLeapfrogJoin(Array(ti1, ti2, ti3))
    ti1.open()
    ti1.open()

    ti2.open()
    ti2.next()
    ti2.open()

    ti3.open()
    ti3.next()
    ti3.open()

    join.init()
    assertJoinEqual(join, Seq(3L), ti1)
  }

  "A join over three relationship" should "be the relationship if all are the same" in {
    val tuples = Array((1L, 2L), (1L, 3L), (2L, 0L), (2L, 3L))

    val ti1 = csrTrieIterator(tuples)
    val ti2 = csrTrieIterator(tuples)
    val ti3 = csrTrieIterator(tuples)

    val join = new MaterializingLeapfrogJoin(Array(ti1, ti2, ti3))
    ti1.open()
    ti1.next()
    ti1.open()

    ti2.open()
    ti2.next()
    ti2.open()

    ti3.open()
    ti3.next()
    ti3.open()

    join.init()
    assertJoinEqual(join, Seq(0L, 3L), ti1)
  }

  "A join over three relationship" should "be the intersection" in {
    val tuples = Array((1L, 2L), (1L, 3L), (2L, 2L), (2L, 3L), (4L, 0L), (4L, 3L), (4L, 5L))

    val ti1 = csrTrieIterator(tuples)
    val ti2 = csrTrieIterator(tuples)
    val ti3 = csrTrieIterator(tuples)

    val join = new MaterializingLeapfrogJoin(Array(ti1, ti2, ti3))
    ti1.open()
    ti1.open()

    ti2.open()
    ti2.next()
    ti2.open()

    ti3.open()
    ti3.next()
    ti3.next()
    ti3.open()

    join.init()
    assertJoinEqual(join, Seq(3L), ti1)
  }

  "A join over multiple relationship" should "be the intersection for level 0 and 1" in {
    val tuples = Array((1L, 2L), (1L, 3L), (2L, 0L), (2L, 3L))

    val ti1 = csrTrieIterator(tuples)
    val ti2 = csrTrieIterator(tuples)

    val join = new MaterializingLeapfrogJoin(Array(ti1, ti2))
    ti1.open()

    ti2.open()
    ti2.open()

    join.init()
    assertJoinEqual(join, Seq(2L), ti1)
  }

  "A join over an empty intersection" should "be atEnd" in {
    val tuples = Array((1L, 2L), (1L, 3L), (2L, 0L), (2L, 4L))

    val ti1 = csrTrieIterator(tuples)
    val ti2 = csrTrieIterator(tuples)

    val join = new MaterializingLeapfrogJoin(Array(ti1, ti2))
    ti1.open()
    ti1.open()

    ti2.open()
    ti2.next()
    ti2.open()

    join.init()
    join.atEnd should be (true)
  }
}
