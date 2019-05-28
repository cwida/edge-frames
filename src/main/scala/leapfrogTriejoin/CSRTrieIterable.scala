package leapfrogTriejoin

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// TODO edges[Long] => edges[Int]
class CSRTrieIterable(val verticeIDs: Array[Long], val edgeIndices: Array[Int], val edges: Array[Long]) extends TrieIterable {


  override def trieIterator: TrieIterator = ???

  // TODO don't forget to translate
  override def iterator: Iterator[InternalRow] = { ???
  }

  override def memoryUsage: Long = { ???
  }

  // For testing
  def getVerticeIDs: Array[Long] = verticeIDs

  // For testing
  def getTranslatedEdges: Array[Long] = edges.map(ei => verticeIDs(ei.toInt))

  // For testing
  def getEdgeIndices: Array[Int] = edgeIndices
}


object CSRTrieIterable {
  def buildBothDirectionsFrom(iterSrcDst: Iterator[InternalRow], iterDstSrc: Iterator[InternalRow]): (CSRTrieIterable, CSRTrieIterable) = {
    if (iterSrcDst.hasNext) {
      val alignedZippedIter = new AlignedZippedIterator(iterSrcDst, iterDstSrc).buffered

      val verticeIDsBuffer = new ArrayBuffer[Long](10000)
      val edgeIndicesSrcBuffer = new ArrayBuffer[Int](10000)
      val edgeIndicesDstBuffer = new ArrayBuffer[Int](10000)
      val edgesDstBuffer = new ArrayBuffer[Long](10000)
      val edgesSrcBuffer = new ArrayBuffer[Long](10000)

      val verticeIDToIndex = new mutable.HashMap[Long, Long]()

      var lastVertice = alignedZippedIter.head(0)

      edgeIndicesSrcBuffer.append(0)
      edgeIndicesDstBuffer.append(0)

      alignedZippedIter.foreach(a => {
        val nextVertice = a(0)
        if (lastVertice != nextVertice) {
          edgeIndicesSrcBuffer.append(edgesDstBuffer.size)
          edgeIndicesDstBuffer.append(edgesSrcBuffer.size)

          verticeIDToIndex.put(lastVertice, verticeIDsBuffer.size)

          verticeIDsBuffer.append(lastVertice)

          lastVertice = nextVertice
        }
        if (a(1) != -1) {
          edgesDstBuffer.append(a(1))
        }
        if (a(2) != -1) {
          edgesSrcBuffer.append(a(2))
        }
      })

      edgeIndicesSrcBuffer.append(edgesDstBuffer.size)
      edgeIndicesDstBuffer.append(edgesSrcBuffer.size)

      verticeIDToIndex.put(lastVertice, verticeIDsBuffer.size)

      verticeIDsBuffer.append(lastVertice)

      // TODO Optimize
      val edgesDstArray = edgesDstBuffer.toArray.map(dst => verticeIDToIndex(dst))
      val edgesSrcArray = edgesSrcBuffer.toArray.map(src => verticeIDToIndex(src))

      val verticeIDs = verticeIDsBuffer.toArray

      (new CSRTrieIterable(verticeIDs, edgeIndicesSrcBuffer.toArray, edgesDstArray), new CSRTrieIterable(verticeIDs, edgeIndicesDstBuffer.toArray,
        edgesSrcArray))
    } else {
      (new CSRTrieIterable(Array[Long](), Array[Int](), Array[Long]()), new CSRTrieIterable(Array[Long](), Array[Int](), Array[Long]()))
    }
  }

  // For testing
  def buildBothDirectionsFrom(srcDst: Array[(Long, Long)], dstSrc: Array[(Long, Long)]): (CSRTrieIterable, CSRTrieIterable) = {
    buildBothDirectionsFrom(srcDst.map(t => new GenericInternalRow(Array[Any](t._1, t._2))).iterator,
      dstSrc.map(t => new GenericInternalRow(Array[Any](t._1, t._2))).iterator)

  }
}