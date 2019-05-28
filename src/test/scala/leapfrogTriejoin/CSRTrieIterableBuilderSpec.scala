package leapfrogTriejoin

import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

// Tests the initializer of CSRTrieIterable
class CSRTrieIterableBuilderSpec extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {

  "VerticeIDs" should "be a mapping from index to original source IDs (specific)" in {
    val tuples1Set = Seq((1L, 1L))
    val tuplesSrcDst = tuples1Set.toArray.sorted
    val tuplesDstSrc = tuples1Set.map(t => (t._2, t._1)).toArray.sorted
    val allVertices = Set(tuplesSrcDst.map(_._1): _*).union(Set(tuplesDstSrc.map(_._1): _*)).toSeq

    val (forwardCSR, backwardCSR) = CSRTrieIterable.buildBothDirectionsFrom(tuplesSrcDst, tuplesDstSrc)

    forwardCSR.verticeIDs should contain theSameElementsInOrderAs allVertices.sorted
    backwardCSR.verticeIDs should contain theSameElementsInOrderAs allVertices.sorted
    forwardCSR.getTranslatedEdges should contain theSameElementsInOrderAs tuplesSrcDst.map(_._2)
    backwardCSR.getTranslatedEdges should contain theSameElementsInOrderAs tuplesDstSrc.map(_._2)

    forwardCSR.getEdgeIndices should contain theSameElementsInOrderAs allVertices.sorted.map(
      v => tuplesSrcDst.map(_._1).lastIndexOf(v)).map(i => if (i == -1) {
      i
    } else {
      i + 1
    }).scanLeft(0)({ case (l, r) => {
      if (r == -1) {
        l
      } else {
        r
      }
    }
    })
    backwardCSR.getEdgeIndices should contain theSameElementsInOrderAs allVertices.sorted.map(
      v => tuplesDstSrc.map(_._1).lastIndexOf(v)).map(i => if (i == -1) {
      i
    } else {
      i + 1
    }).scanLeft(0)({ case (l, r) => {
      if (r == -1) {
        l
      } else {
        r
      }
    }
    })
  }

  "VerticeIDs" should "be a mapping from index to original source IDs" in {
    import math.Ordering._
    val positiveIntTuples = Gen.buildableOf[Set[(Long, Long)], (Long, Long)](Gen.zip(Gen.posNum[Long], Gen.posNum[Long]))

    forAll(positiveIntTuples) { (tuples1Set) =>
      whenever(List(tuples1Set).forall(t => t.forall(t => t._1 > 0 && t._2 > 0))) { // Sad way to ensure numbers are actually positive
        val tuplesSrcDst = tuples1Set.toArray.sorted
        val tuplesDstSrc = tuples1Set.map(t => (t._2, t._1)).toArray.sorted
        val allVertices = Set(tuplesSrcDst.map(_._1): _*).union(Set(tuplesDstSrc.map(_._1): _*)).toSeq

        val (forwardCSR, backwardsCSR) = CSRTrieIterable.buildBothDirectionsFrom(tuplesSrcDst, tuplesDstSrc)

        forwardCSR.getVerticeIDs should contain theSameElementsInOrderAs allVertices.sorted
        backwardsCSR.getVerticeIDs should contain theSameElementsInOrderAs allVertices.sorted
      }
    }
  }

  "edgesDst and edgesSrc" should "equal the second column of the respective input iterator" in {
    import math.Ordering._
    val positiveIntTuples = Gen.buildableOf[Set[(Long, Long)], (Long, Long)](Gen.zip(Gen.posNum[Long], Gen.posNum[Long]))

    forAll(positiveIntTuples) { (tuples1Set) =>
      whenever(List(tuples1Set).forall(t => t.forall(t => t._1 > 0 && t._2 > 0))) { // Sad way to ensure numbers are actually positive
        val tuplesSrcDst = tuples1Set.toArray.sorted
        val tuplesDstSrc = tuples1Set.map(t => (t._2, t._1)).toArray.sorted
        val allVertices = Set(tuplesSrcDst.map(_._1): _*).union(Set(tuplesDstSrc.map(_._1): _*)).toSeq

        val (forwardCSR, backwardsCSR) = CSRTrieIterable.buildBothDirectionsFrom(tuplesSrcDst, tuplesDstSrc)


        forwardCSR.getTranslatedEdges should contain theSameElementsInOrderAs tuplesSrcDst.map(_._2)
        backwardsCSR.getTranslatedEdges should contain theSameElementsInOrderAs tuplesDstSrc.map(_._2)
      }
    }
  }

  "edgeIndicesSrc and edgeIndicesDst" should "equal the last index + 1 foreach vertice in the first attribute or the last index of the " +
    "vertice before if it is not included" in {
    import math.Ordering._
    val positiveIntTuples = Gen.nonEmptyBuildableOf[Set[(Long, Long)], (Long, Long)](Gen.zip(Gen.posNum[Long], Gen.posNum[Long]))

    forAll(positiveIntTuples) { (tuples1Set) =>
      whenever(List(tuples1Set).forall(t => t.forall(t => t._1 > 0 && t._2 > 0))) { // Sad way to ensure numbers are actually positive
        val tuplesSrcDst = tuples1Set.toArray.sorted
        val tuplesDstSrc = tuples1Set.map(t => (t._2, t._1)).toArray.sorted
        val allVertices = Set(tuplesSrcDst.map(_._1): _*).union(Set(tuplesDstSrc.map(_._1): _*)).toSeq

        val (forwardCSR, backwardsCSR) = CSRTrieIterable.buildBothDirectionsFrom(tuplesSrcDst, tuplesDstSrc)

        forwardCSR.getEdgeIndices should contain theSameElementsInOrderAs allVertices.sorted
          .map(v => tuplesSrcDst.map(_._1).lastIndexOf(v)) // last index
          .map(i => if (i == -1) { // last index plus 1 if in array
          i
        } else {
          i + 1
        })
          .scanLeft(0)({ case (l, r) => { // repeat last element instead of -1 (not in array)
            if (r == -1) {
              l
            } else {
              r
            }
          }
          })
        backwardsCSR.getEdgeIndices should contain theSameElementsInOrderAs allVertices.sorted.map(
          v => tuplesDstSrc.map(_._1).lastIndexOf(v))
          .map(i => if (i == -1) {
            i
          } else {
            i + 1
          })
          .scanLeft(0)({ case (l, r) => {
            if (r == -1) {
              l
            } else {
              r
            }
          }
          })
      }
    }
  }

}
