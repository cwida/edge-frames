package leapfrogTriejoin

import org.scalacheck.Gen
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class LeapfrogJoinSpec extends FlatSpec with GeneratorDrivenPropertyChecks with Matchers {

  def assertJoinEqual(join: LeapfrogJoin, values: Seq[Long]) = {
    for (v <- values) {
      assert(join.key == v)
      join.leapfrogNext()
    }
    assert(join.atEnd)
  }

  def trieIteratorFromUnaryRelationship(unaryRel: Array[Long]): TrieIterator = {
    val ti = new TreeTrieIterator(unaryRel.map(v => (v, 0L)))
    ti.open()
    ti
  }

  "A empty join " should "throw an error on creation" in {
    assertThrows[IllegalArgumentException](new LeapfrogJoin(Array[LinearIterator]()))
  }

  "A join over an empty relationship" should "at end" in {
    val join = new LeapfrogJoin(Array(new TreeTrieIterator(Array[(Long, Long)]())))
    join.init()
    assert(join.atEnd)
  }

  "A join over a single relationship" should "equal the relationship" in {
    val values = Array[Long](1, 2, 3)

    val join = new LeapfrogJoin(Array(trieIteratorFromUnaryRelationship(values)))
    join.init()
    assertJoinEqual(join, values)
  }

  "A join over multiple relationship" should "be the intersection" in {
    val values1 = Array[Long](1, 2, 4)
    val values2 = Array[Long](3, 4)
    val join = new LeapfrogJoin(Array(trieIteratorFromUnaryRelationship(values1), trieIteratorFromUnaryRelationship(values2)))
    join.init()
    assertJoinEqual(join, values1.intersect(values2))
  }

  "A join over an empty intersection" should "be atEnd" in {
    val values1 = Array[Long](1)
    val values2 = Array[Long](2)
    val join = new LeapfrogJoin(Array(trieIteratorFromUnaryRelationship(values1),
      trieIteratorFromUnaryRelationship(values2)))
    join.init()
    assert(join.atEnd)
  }


  "sortIterators1" should "sort the iterators" in {
    val join = new LeapfrogJoin(Array(
      trieIteratorFromUnaryRelationship(Array(3)),
      trieIteratorFromUnaryRelationship(Array(22)),
      trieIteratorFromUnaryRelationship(Array(1))))

    val expected = List(join.iterators:_*)
    val smallestIndex = join.sortIterators()
    rotateSeq(join.iterators, smallestIndex).map(_.key) should contain theSameElementsInOrderAs expected.sortBy(_.key).map(_.key)
  }

  private def rotateSeq[A](v : Seq[A], shift: Int): Seq[A]= {
    v.drop(shift) ++ v.take(shift)
  }

  "sortIterators" should "sort the iterators" in {

    val positiveInts1 = Gen.nonEmptyListOf(Gen.posNum[Long])
    val positiveInts2 = Gen.nonEmptyListOf(Gen.posNum[Long])
    val positiveInts3 = Gen.nonEmptyListOf(Gen.posNum[Long])

    forAll(positiveInts1, positiveInts2, positiveInts3) { (rel1, rel2, rel3) =>
      whenever(rel1.union(rel2).union(rel3).forall(_ > 0)) {
        val join = new LeapfrogJoin(Array(
          trieIteratorFromUnaryRelationship(rel1.toArray),
          trieIteratorFromUnaryRelationship(rel2.toArray),
          trieIteratorFromUnaryRelationship(rel3.toArray)))

        val expected = List(join.iterators:_*)
        val smallestIndex = join.sortIterators()
        whenever(!join.atEnd) {
          rotateSeq(join.iterators, smallestIndex).map(_.key) should contain theSameElementsInOrderAs expected.sortBy(_.key).map(_.key)
        }
        }
    }
  }
}
