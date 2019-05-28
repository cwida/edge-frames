package leapfrogTriejoin

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class CSRTrieIterable(iterSrcDst: Iterator[InternalRow], iterDstSrc: Iterator[InternalRow]) extends TrieIterable {
  private[this] val (verticeIDs, edgeIndicesSrc, edgeIndicesDst, edgesDst, edgesSrc) = build(iterSrcDst, iterDstSrc)

  private def build(iterSrcDst: Iterator[InternalRow], iterDstSrc: Iterator[InternalRow]): (Array[Long],
    Array[Long], Array[Long], Array[Long], Array[Long]) = {
    if (iterSrcDst.hasNext) {
      val alignedZippedIter = new AlignedZippedIterator(iterSrcDst, iterDstSrc).buffered

      val verticeIDsBuffer = new ArrayBuffer[Long](10000)
      val edgeIndicesSrcBuffer = new ArrayBuffer[Long](10000)
      val edgeIndicesDstBuffer = new ArrayBuffer[Long](10000)
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

      (verticeIDsBuffer.toArray, edgeIndicesSrcBuffer.toArray, edgeIndicesDstBuffer.toArray, edgesDstArray, edgesSrcArray)
    } else {
      (Array[Long](), Array[Long](), Array[Long](), Array[Long](), Array[Long]())
    }
  }

  // For testing
  def this(srcDst: Array[(Long, Long)], dstSrc: Array[(Long, Long)]) {
    this(srcDst.map(t => new GenericInternalRow(Array[Any](t._1, t._2))).iterator,
      dstSrc.map(t => new GenericInternalRow(Array[Any](t._1, t._2))).iterator)
  }

  val arrayTrieIterable = new ArrayTrieIterable(iterSrcDst)

  override def trieIterator: TrieIterator = {
    arrayTrieIterable.trieIterator
  }

  override def iterator: Iterator[InternalRow] = {
    arrayTrieIterable.iterator
  }

  override def memoryUsage: Long = {
    arrayTrieIterable.memoryUsage
  }

  // For testing
  def getVerticeIDs: Array[Long] = verticeIDs

  // For testing
  def getTranslatedEdgesSrc: Array[Long] = edgesSrc.map(ei => verticeIDs(ei.toInt))

  // For testing
  def getTranslatedEdgesDst: Array[Long] = edgesDst.map(ei => verticeIDs(ei.toInt))

  // For testing
  def getTranslatedEdgeIndiceSrc: Array[Long] = edgeIndicesSrc

  // For testing
  def getTranslatedEdgeIndiceDst: Array[Long] = edgeIndicesDst

}
