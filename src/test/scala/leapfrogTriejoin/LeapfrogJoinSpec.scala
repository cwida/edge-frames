package leapfrogTriejoin

import org.scalacheck.Gen
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class LeapfrogJoinSpec extends FlatSpec with GeneratorDrivenPropertyChecks with Matchers {

  def assertJoinEqual(join: LeapfrogJoin, values: Seq[Int]) = {
    for (v <- values) {
      assert(join.key == v)
      join.leapfrogNext()
    }
    assert(join.atEnd)
  }

  def trieIteratorFromUnaryRelationship(unaryRel: Array[Int]): TrieIterator = {
    val ti = new TreeTrieIterator(unaryRel.map(v => (v, 0)))
    ti.open()
    ti
  }

  "A empty join " should "throw an error on creation" in {
    assertThrows[IllegalArgumentException](new LeapfrogJoin(Array[LinearIterator]()))
  }

  "A join over an empty relationship" should "at end" in {
    val join = new LeapfrogJoin(Array(new TreeTrieIterator(Array[(Int, Int)]())))
    join.init()
    assert(join.atEnd)
  }

  "A join over a single relationship" should "equal the relationship" in {
    val values = Array(1, 2, 3)

    val join = new LeapfrogJoin(Array(trieIteratorFromUnaryRelationship(values)))
    join.init()
    assertJoinEqual(join, values)
  }

  "A join over multiple relationship" should "be the intersection" in {
    val values1 = Array(1, 2, 4)
    val values2 = Array(3, 4)
    val join = new LeapfrogJoin(Array(trieIteratorFromUnaryRelationship(values1), trieIteratorFromUnaryRelationship(values2)))
    join.init()
    assertJoinEqual(join, values1.intersect(values2))
  }

  "A join over an empty intersection" should "be atEnd" in {
    val values1 = Array(1)
    val values2 = Array(2)
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

    val expected = join.iterators.sortBy(_.key)
    join.sortIterators()
    join.iterators.map(_.key) should contain theSameElementsInOrderAs expected.map(_.key)
  }

  "sortIterators" should "sort the iterators" in {

    val positiveInts1 = Gen.nonEmptyListOf(Gen.posNum[Int])
    val positiveInts2 = Gen.nonEmptyListOf(Gen.posNum[Int])
    val positiveInts3 = Gen.nonEmptyListOf(Gen.posNum[Int])

    forAll(positiveInts1, positiveInts2, positiveInts3) { (rel1, rel2, rel3) =>
      whenever(rel1.union(rel2).union(rel3).forall(_ > 0)) {
        val join = new LeapfrogJoin(Array(
          trieIteratorFromUnaryRelationship(rel1.toArray),
          trieIteratorFromUnaryRelationship(rel2.toArray),
          trieIteratorFromUnaryRelationship(rel3.toArray)))

        val expected = join.iterators.sortBy(_.key)
        join.sortIterators()
        join.iterators should contain theSameElementsInOrderAs expected
      }
    }
  }
}
