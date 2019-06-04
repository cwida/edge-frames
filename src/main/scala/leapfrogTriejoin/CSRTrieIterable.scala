package leapfrogTriejoin

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// TODO edges[Long] => edges[Int]
class CSRTrieIterable(private[this] val verticeIDs: Array[Long],
                      private[this] val edgeIndices: Array[Int],
                      private[this] val edges: Array[Int]) extends TrieIterable {

  override def trieIterator: TrieIteratorImpl = {
    new TrieIteratorImpl()
  }


  // TODO can I remove 0 elements in the first level somehow?
  class TrieIteratorImpl() extends TrieIterator {
    private[this] var isAtEnd = verticeIDs.length == 0

    private[this] var depth = -1

    private[this] var srcPosition = 0
    if (!isAtEnd && edgeIndices(srcPosition) == edgeIndices(srcPosition + 1)) {
      moveToNextSrcPosition()
    }

    private[this] var dstPosition = 0

    override def open(): Unit = {
      assert(!isAtEnd, "open cannot be called when atEnd")
      depth += 1

      if (depth == 0) {
        srcPosition = 0
        if (edgeIndices(srcPosition) == edgeIndices(srcPosition + 1)) {
          moveToNextSrcPosition()  // TODO optimize by rememebering first src position
        }
      } else if (depth == 1) {  // TODO predicatable
        dstPosition = edgeIndices(srcPosition)
      }

      assert(!isAtEnd, "open cannot be called when atEnd")
      assert(depth < 2)
    }

    override def up(): Unit = {
      assert(depth == 1 || depth == 0, s"Depth was $depth")
      depth -= 1
      isAtEnd = false
    }

    override def key: Long = {
      assert(depth == 0 || depth == 1, "Wrong key call")
      if (depth == 0) {
        srcPosition
      } else {
        edges(dstPosition)
      }
    }

    override def next(): Unit = {
      assert(!atEnd)
      if (depth == 0) {
        moveToNextSrcPosition()
        isAtEnd = srcPosition == edgeIndices.length - 1
      } else {
        dstPosition += 1
        isAtEnd = dstPosition == edgeIndices(srcPosition + 1)  // TODO factor out
      }
    }

    override def atEnd: Boolean = isAtEnd

    // TODO int / long problem
    override def seek(key: Long): Boolean = {
      assert(!atEnd)
      if (depth == 0) {
        srcPosition = key.toInt
        if (srcPosition < edgeIndices.length - 1 &&
          edgeIndices(srcPosition) == edgeIndices(srcPosition + 1)) {
          moveToNextSrcPosition()
        }
        isAtEnd = srcPosition >= edgeIndices.length - 1
        isAtEnd
      } else {
        dstPosition = ArraySearch.find(edges.map(_.toLong), key, edgeIndices(srcPosition), edgeIndices(srcPosition + 1))  // TODO long
        // int problem
        isAtEnd = dstPosition == edgeIndices(srcPosition + 1)
        isAtEnd
      }
    }

    private def moveToNextSrcPosition(): Unit = {
      var indexToSearch = edgeIndices(srcPosition + 1) // TODO linear search is best? or galloping or binary?

      do {
        srcPosition += 1
      } while (srcPosition < edgeIndices.length - 1 && edgeIndices(srcPosition + 1) == indexToSearch)  // TODO sentry element
    }

    def translate(key: Int): Long = {
      verticeIDs(key)
    }

    def translate(keys: Array[Int]): Array[Long] = {
      keys.map(verticeIDs(_))  // TODO int long sort out
      // TODO inneficient
    }

  }

  def translate(key: Int): Long = {
    verticeIDs(key)
  }

  def translate(keys: Array[Int]): Array[Long] = {
    keys.map(verticeIDs(_))
  }

  override def iterator: Iterator[InternalRow] = {
    var currentSrcPosition = 0
    var currentDstPosition = 0

    new Iterator[InternalRow] {
      override def hasNext: Boolean = {
        currentDstPosition < edges.length
      }

      override def next(): InternalRow = {
        while (currentDstPosition >= edgeIndices(currentSrcPosition + 1)) {
          currentSrcPosition += 1
        }
        val r = InternalRow(verticeIDs(currentSrcPosition), verticeIDs(edges(currentDstPosition)))
        currentDstPosition += 1
        r
      }
    }
  }

  override def memoryUsage: Long = {
    -1 // TODO implement
  }

  // For testing
  def getVerticeIDs: Array[Long] = {
    verticeIDs
  }

  // For testing
  def getTranslatedEdges: Array[Long] = {
    edges.map(ei => verticeIDs(ei))
  }

  // For testing
  def getEdgeIndices: Array[Int] = {
    edgeIndices
  }
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

      val verticeIDToIndex = new mutable.HashMap[Long, Int]()

      var lastVertice = alignedZippedIter.head(0)

      edgeIndicesSrcBuffer.append(0)
      edgeIndicesDstBuffer.append(0)

      alignedZippedIter.foreach(a => {
        val nextVertice = a(0)
        if (lastVertice != nextVertice) {
          edgeIndicesSrcBuffer.append(edgesDstBuffer.size)
          edgeIndicesDstBuffer.append(edgesSrcBuffer.size)

          verticeIDToIndex.put(lastVertice, verticeIDsBuffer.size)
//          println(lastVertice)

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
      (new CSRTrieIterable(Array[Long](), Array[Int](), Array[Int]()),
        new CSRTrieIterable(Array[Long](), Array[Int](), Array[Int]()))
    }
  }

  // For testing
  def buildBothDirectionsFrom(srcDst: Array[(Long, Long)], dstSrc: Array[(Long, Long)]): (CSRTrieIterable, CSRTrieIterable) = {
    buildBothDirectionsFrom(srcDst.map(t => new GenericInternalRow(Array[Any](t._1, t._2))).iterator,
      dstSrc.map(t => new GenericInternalRow(Array[Any](t._1, t._2))).iterator)
  }
}