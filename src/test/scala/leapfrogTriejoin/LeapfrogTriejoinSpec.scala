package leapfrogTriejoin

import java.io.{BufferedReader, File, FileReader}
import java.util.stream.Collectors

import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import testing.Utils
import leapfrogTriejoin.implicits._
import collection.JavaConverters._


class LeapfrogTriejoinSpec extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {

  private def assertJoinEqual(join: LeapfrogTriejoin, values: Set[List[Long]]) = {
    for (i <- 0 until values.size) {
      assert(!join.atEnd)
      values should contain(join.next())
    }
    assert(join.atEnd)
  }

  private def parseRegressionTestDataset(ds: String): Array[(Long, Long)] = {
    ds.replace("[", "")
      .replace("]", "")
      .split("\n")
      .map(_.trim)
      .filter(_ != "")
      .map(_.split(","))
      .map(_.map(_.toLong))
      .map(l => (l(0), l(1)))
  }

  private def parseRegressionTestDatasetFromFile(filePath: String): Array[(Long, Long)] = {
    val reader = new BufferedReader(new FileReader(filePath))

    val lines = reader.lines().collect(Collectors.toList()).asScala
    lines
      .map(_.trim)
      .filter(_ != "")
      .map(_.split(","))
      .map(_.map(_.trim.toLong))
      .map(l => (l(0), l(1))).toArray
  }

  "A join on a single relationship" should "be the relationship" in {
    val tuples = Array[(Long, Long)]((1, 1), (2, 1))
    val rel = new EdgeRelationship(("a", "b"))
    val trieIterator = new TreeTrieIterator(tuples)
    val join = new LeapfrogTriejoin(Map(rel -> trieIterator), List("a", "b"))
    assertJoinEqual(join, tuples.map(t => List(t._1, t._2)).toSet)
  }

  "An empty relationship " should "produce an empty result" in {
    val tuples = Array[(Long, Long)]()
    val rel = new EdgeRelationship(("a", "b"))
    val trieIterator = new TreeTrieIterator(tuples)
    val join = new LeapfrogTriejoin(Map(rel -> trieIterator), List("a", "b"))
    assert(join.atEnd)
  }

  "A join on the first attribute" should "be the intersection on the first attribute" in {
    val positiveIntTuples = Gen.buildableOf[Set[(Long, Long)], (Long, Long)](Gen.zip(Gen.posNum[Long], Gen.posNum[Long]))

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
    val tuples1 = Array[(Long, Long)]((1, 2), (3, 3), (4, 2), (5, 1))
    val tuples2 = Array[(Long, Long)]((2, 2), (3, 4), (5, 2))

    val rel1 = new EdgeRelationship(("a", "b"))
    val rel2 = new EdgeRelationship(("c", "b"))
    val trieIterator1 = new TreeTrieIterator(tuples1)
    val trieIterator2 = new TreeTrieIterator(tuples2)
    val join = new LeapfrogTriejoin(Map(rel1 -> trieIterator1, rel2 -> trieIterator2), List("a", "c", "b"))

    assertJoinEqual(join, Set(List(1, 2, 2), List(1, 5, 2), List(4, 2, 2), List(4, 5, 2)))
  }

  "Triangle joins" should "work" in {
    val tuples1 = Array[(Long, Long)]((1, 2), (3, 3), (4, 7), (5, 1))
    val tuples2 = Array[(Long, Long)]((2, 4), (3, 5), (5, 2))
    val tuples3 = Array[(Long, Long)]((1, 2), (3, 3), (3, 5), (5, 8))

    val rel1 = new EdgeRelationship(("a", "b"))
    val rel2 = new EdgeRelationship(("b", "c"))
    val rel3 = new EdgeRelationship(("a", "c"))
    val trieIterator1 = new TreeTrieIterator(tuples1)
    val trieIterator2 = new TreeTrieIterator(tuples2)
    val trieIterator3 = new TreeTrieIterator(tuples3)
    val join = new LeapfrogTriejoin(Map(rel1 -> trieIterator1, rel2 -> trieIterator2, rel3 -> trieIterator3), List("a", "b", "c"))

    assertJoinEqual(join, Set(List(3, 3, 5)))
  }

  val regression1Dataset =
    """
      |[4,7]
      |[4,16]
      |[4,19]
      |[16,4]
      |[16,7]
      |[16,19]
      |[19,4]
      |[19,16]
    """.stripMargin

  "Regression 1: " should "work if LeapFrog Join actually sorts its iterators" in {
    val ds = parseRegressionTestDataset(regression1Dataset)

    val edges: List[EdgeRelationship] = ('a' to 'd')
      .combinations(2)
      .filter(l => l(0) < l(1))
      .map(l => new EdgeRelationship((s"${l(0)}", s"${l(1)}")))
      .toList

    val rels: List[TrieIterator] = edges
      .map(e => new ArrayTrieIterable(ds).trieIterator)

    val join = new LeapfrogTriejoin(edges.zip(rels).toMap, Seq("a", "b", "c", "d"))

    val result = join.toList
    val resultAsSets = result.map(_.toSet)

    resultAsSets should not contain Set(19, 4, 16, 7)
  }

  val regression2Dataset =
    """
      |0,1
      |0,2
      |0,3
      |0,4
      |0,5
      |1,0
      |1,2
      |1,4
      |1,5
      |1,15
    """.stripMargin


  val regression2ExpectedResult = Array(
    Array(0, 1, 2),
    Array(0, 1, 5),
    Array(0, 1, 4)
  )

  val regression2ExpectedResultNoFilter = Array(
    Array(0, 1, 2),
    Array(0, 1, 4),
    Array(0, 1, 5),
    Array(1, 0, 2),
    Array(1, 0, 4),
    Array(1, 0, 5)
  )

  "Regression 2: " should "test CSRTrieIterables on first 10 edges of Amazon0302" in {
    val ds = parseRegressionTestDataset(regression2Dataset)
    val dsReversed = ds.map(t => (t._2, t._1)).sorted

    val edges: List[EdgeRelationship] = ('a' to 'c')
      .combinations(2)
      .filter(l => l(0) < l(1))
      .map(l => new EdgeRelationship((s"${l(0)}", s"${l(1)}")))
      .toList

      val (ti1, _) = CSRTrieIterable.buildBothDirectionsFrom(ds, dsReversed)
//    val ti1 = new ArrayTrieIterable(ds)

    val rels: List[TrieIterator] = edges
      .map(e => ti1.trieIterator)

    val join = new LeapfrogTriejoin(edges.zip(rels).toMap, Seq("a", "b", "c"), smallerThanFilter = true)

    val result = join.toList

    result should contain theSameElementsAs regression2ExpectedResult
  }

  "Regression 2: " should "test CSRTrieIterables on first 10 edges of Amazon0302 with a differen variable ordering" in {
    val ds = parseRegressionTestDataset(regression2Dataset)
    val dsReversed = ds.map(t => (t._2, t._1)).sorted

    val edges: List[EdgeRelationship] = List(new EdgeRelationship(("a", "b")),
      new EdgeRelationship(("c", "b")), new EdgeRelationship(("c", "a")))

    val (ti1, ti2) = CSRTrieIterable.buildBothDirectionsFrom(ds, dsReversed)
//    val ti1 = new ArrayTrieIterable(ds)
//    val ti2 = new ArrayTrieIterable(dsReversed)


    val rels: List[TrieIterator] = List(ti1.trieIterator, ti2.trieIterator, ti2.trieIterator)

    val join = new LeapfrogTriejoin(edges.zip(rels).toMap, Seq("c", "a", "b"))

    val result = join.toList
    val resultReordered = result.map(a => Array(a(1), a(2), a(0)))

    resultReordered should contain theSameElementsAs regression2ExpectedResultNoFilter
  }
}
