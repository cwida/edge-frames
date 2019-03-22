package leapfrogTriejoin

import org.scalatest.{FlatSpec, Matchers}

class LeapfrogTriejoinSpec extends FlatSpec with Matchers {

  def assertJoinEqual(join: LeapfrogTriejoin, values: Seq[Seq[Int]]) ={
    for (v <- values) {
      assert(!join.atEnd)
      join.next should contain theSameElementsInOrderAs v
    }
    assert(join.atEnd)
  }

  "A join on a single relationship" should "be the relationship" in  {
    val tuples = Array((1, 1), (2, 1))
    val rel = new EdgeRelationship(("a", "b"), tuples)
    val trieIterator = new TrieIterator(rel)
    val join = new LeapfrogTriejoin(List(trieIterator), List("a", "b"))
    assertJoinEqual(join, tuples.map(t => List(t._1, t._2)))
  }

  "An empty relationship " should "produce an empty result" in {
    val tuples = Array[(Int, Int)]()
    val rel = new EdgeRelationship(("a", "b"), tuples)
    val trieIterator = new TrieIterator(rel)
    val join = new LeapfrogTriejoin(List(trieIterator), List("a", "b"))
    assert(join.atEnd)
  }

  "A join on the first attribute" should "be the intersection on the first attribute" in {
    val tuples1 = Array[(Int, Int)]((1, 2), (3, 2), (4, 2), (5, 1))
    val tuples2 = Array[(Int, Int)]((2, 2), (3, 4), (5, 2))
    val rel1 = new EdgeRelationship(("a", "b"), tuples1)
    val rel2 = new EdgeRelationship(("a", "c"), tuples2)
    val trieIterator1 = new TrieIterator(rel1)
    val trieIterator2 = new TrieIterator(rel2)
    val join = new LeapfrogTriejoin(List(trieIterator1, trieIterator2), List("a", "b", "c"))

    assertJoinEqual(join, List(List(3, 2, 4), List(5, 1, 2)))
  }

  "A join on the second attribute" should "be the intersection on the second attribute" in {
    val tuples1 = Array[(Int, Int)]((1, 2), (3, 3), (4, 2), (5, 1))
    val tuples2 = Array[(Int, Int)]((2, 2), (3, 4), (5, 2))

    val rel1 = new EdgeRelationship(("a", "b"), tuples1)
    val rel2 = new EdgeRelationship(("c", "b"), tuples2)
    val trieIterator1 = new TrieIterator(rel1)
    val trieIterator2 = new TrieIterator(rel2)
    val join = new LeapfrogTriejoin(List(trieIterator1, trieIterator2), List("a", "c", "b"))

    assertJoinEqual(join, List(List(1, 2, 2), List(1, 5, 2), List(4, 2, 2), List(4, 5, 2)))
  }

}
