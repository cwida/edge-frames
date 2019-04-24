package leapfrogTriejoin

import org.scalatest.FlatSpec

class LeapfrogJoinSpec extends FlatSpec {

  def assertJoinEqual(join: LeapfrogJoin, values: Seq[Int]) ={
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

  // TODO express interator as scala iterator and use iterator/list equal?
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
}
