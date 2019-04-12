package leapfrogTriejoin

import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class LeapfrogTriejoinSpec extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {

  def assertJoinEqual(join: LeapfrogTriejoin, values: Set[List[Int]]) ={
    for (i <- 0 until values.size) {
      assert(!join.atEnd)
      values should contain (join.next())
    }
    assert(join.atEnd)
  }

  "A join on a single relationship" should "be the relationship" in  {
    val tuples = Array((1, 1), (2, 1))
    val rel = new EdgeRelationship(("a", "b"))
    val trieIterator = new TreeTrieIterator(tuples)
    val join = new LeapfrogTriejoin(Map(rel -> trieIterator), List("a", "b"))
    assertJoinEqual(join, tuples.map(t => List(t._1, t._2)).toSet)
  }

  "An empty relationship " should "produce an empty result" in {
    val tuples = Array[(Int, Int)]()
    val rel = new EdgeRelationship(("a", "b"))
    val trieIterator = new TreeTrieIterator(tuples)
    val join = new LeapfrogTriejoin(Map(rel -> trieIterator), List("a", "b"))
    assert(join.atEnd)
  }

  "A join on the first attribute" should "be the intersection on the first attribute" in {
    val positiveIntTuples = Gen.buildableOf[Set[(Int, Int)], (Int, Int)](Gen.zip(Gen.posNum[Int], Gen.posNum[Int]))

    forAll(positiveIntTuples, positiveIntTuples) { (tuples1Set, tuples2Set) =>
      whenever(List(tuples1Set, tuples2Set).forall(t => t.forall(t => t._1 > 0 && t._2 > 0))) { // Sad way to ensure numbers are actually positive
        val tuples1 = tuples1Set.toArray.sorted
        val tuples2 = tuples2Set.toArray.sorted

        val rel1 = new EdgeRelationship(("a", "b"))
        val rel2 = new EdgeRelationship(("a", "c"))
        val trieIterator1 = new TreeTrieIterator(tuples1)
        val trieIterator2 = new TreeTrieIterator(tuples2)
        val join = new LeapfrogTriejoin(Map(rel1 -> trieIterator1, rel2 -> trieIterator2), List("a", "b", "c"))

        val expectedResult = tuples1.flatMap(t1 => tuples2.filter(t2 => t2._1 == t1._1).map(t2 => List(t1._1, t1._2, t2._2))).toSet
        assertJoinEqual(join, expectedResult)
      }
    }
  }


  "A join on the second attribute" should "be the intersection on the second attribute" in {
    val tuples1 = Array[(Int, Int)]((1, 2), (3, 3), (4, 2), (5, 1))
    val tuples2 = Array[(Int, Int)]((2, 2), (3, 4), (5, 2))

    val rel1 = new EdgeRelationship(("a", "b"))
    val rel2 = new EdgeRelationship(("c", "b"))
    val trieIterator1 = new TreeTrieIterator(tuples1)
    val trieIterator2 = new TreeTrieIterator(tuples2)
    val join = new LeapfrogTriejoin(Map(rel1 -> trieIterator1, rel2 -> trieIterator2), List("a", "c", "b"))

    assertJoinEqual(join, Set(List(1, 2, 2), List(1, 5, 2), List(4, 2, 2), List(4, 5, 2)))
  }

  "Triangle joins" should "work" in {
    val tuples1 = Array[(Int, Int)]((1, 2), (3, 3), (4, 7), (5, 1))
    val tuples2 = Array[(Int, Int)]((2, 4), (3, 5), (5, 2))
    val tuples3 = Array[(Int, Int)]((1, 2), (3, 3), (3, 5), (5, 8))

    val rel1 = new EdgeRelationship(("a", "b"))
    val rel2 = new EdgeRelationship(("b", "c"))
    val rel3 = new EdgeRelationship(("a", "c"))
    val trieIterator1 = new TreeTrieIterator(tuples1)
    val trieIterator2 = new TreeTrieIterator(tuples2)
    val trieIterator3 = new TreeTrieIterator(tuples3)
    val join = new LeapfrogTriejoin(Map(rel1 -> trieIterator1, rel2 -> trieIterator2, rel3 -> trieIterator3), List("a", "b", "c"))

    assertJoinEqual(join, Set(List(3, 3, 5)))
  }

}
