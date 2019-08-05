package leapfrogTriejoin

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// TODO serializing could be improved by sending common parts of the array only once
class CSRTrieIterable(private[this] val verticeIDs: Array[Long],
                      val edgeIndices: Array[Int],
                      private[this] val edges: Array[Long]) extends TrieIterable with Serializable {

  override def trieIterator: TrieIteratorImpl = {
    new TrieIteratorImpl()
  }

  class TrieIteratorImpl() extends TrieIterator {

    private[this] var isAtEnd = verticeIDs.length == 0

    private[this] var depth = -1

    private[this] var srcPosition = 0
    if (!isAtEnd && edgeIndices(srcPosition) == edgeIndices(srcPosition + 1)) {
      moveToNextSrcPosition()
    }
    private[this] val firstSourcePosition = srcPosition

    private[this] var dstPosition = 0

    private[this] var keyValue = 0L

    override def open(): Unit = {
      assert(!isAtEnd, "open cannot be called when atEnd")
      depth += 1

      if (depth == 0) {
        srcPosition = firstSourcePosition
        keyValue = srcPosition.toLong
      } else if (depth == 1) { // TODO predicatable
        dstPosition = edgeIndices(srcPosition)
        keyValue = edges(dstPosition)
      }

      assert(!isAtEnd, "open cannot be called when atEnd")
      assert(depth < 2)
    }

    override def up(): Unit = {
      assert(depth == 1 || depth == 0, s"Depth was $depth")
      depth -= 1
      isAtEnd = false
      if (depth == 0) {
        keyValue = srcPosition
      }
    }

    override def key: Long = {
      keyValue
    }

    override def next(): Unit = {
      assert(!atEnd)
      if (depth == 0) {
        moveToNextSrcPosition()
        isAtEnd = srcPosition == edgeIndices.length - 1
        keyValue = srcPosition.toLong
      } else {
        dstPosition += 1
        isAtEnd = dstPosition == edgeIndices(srcPosition + 1) // edgeIndices(srcPosition + 1) should not be factored out, it does not
        // look like this improves performance (looks!)
        if (!isAtEnd) {
          keyValue = edges(dstPosition)
        }
      }
    }

    override def atEnd: Boolean = {
      isAtEnd
    }

    override def seek(key: Long): Boolean = {
      assert(!atEnd)
      if (depth == 0) {
        srcPosition = key.toInt
        if (srcPosition < edgeIndices.length - 1 && // TODO srcPosition should never be bigger than edgeIndices.lenght -  1, investigate
          edgeIndices(srcPosition) == edgeIndices(srcPosition + 1)) { // TODO does srcPosition < edgeIndices.length - 1 ruin
          // predicatability?
          moveToNextSrcPosition()
        }
        isAtEnd = srcPosition >= edgeIndices.length - 1
        this.keyValue = srcPosition.toLong
        isAtEnd
      } else {
        dstPosition = ArraySearch.find(edges, key, dstPosition, edgeIndices(srcPosition + 1))
        isAtEnd = dstPosition == edgeIndices(srcPosition + 1)
        if (!isAtEnd) {
          this.keyValue = edges(dstPosition)
        }
        isAtEnd
      }
    }

    private def moveToNextSrcPosition(): Unit = {
      var indexToSearch = edgeIndices(srcPosition + 1) // A linear search is ideal, see log 05.06

      do {
        srcPosition += 1
      } while (srcPosition < edgeIndices.length - 1 && edgeIndices(srcPosition + 1) == indexToSearch) // TODO sentry element
    }

    // For testing
    def translate(key: Int): Long = {
      verticeIDs(key)
    }

    def translate(keys: Array[Long]): Array[Long] = {
      var i = 0
      while (i < keys.length) {
        keys(i) = verticeIDs(keys(i).toInt)
        i += 1
      }
      keys
    }

    override def estimateSize: Int = {
      if (depth == 0) {
        Integer.MAX_VALUE
      } else {
        edgeIndices(srcPosition + 1) - edgeIndices(srcPosition)
      }
    }

    override def getDepth: Int = {
      depth
    }

    override def clone(): AnyRef = {
      val c = new TrieIteratorImpl()
      c.copy(isAtEnd, depth, srcPosition, dstPosition, keyValue)
      c
    }

    private def copy(atEnd: Boolean, depth: Int, srcPosition: Int, dstPosition: Int, keyValue: Long) {
      isAtEnd = atEnd
      this.depth = depth
      this.srcPosition = srcPosition
      this.dstPosition = dstPosition
      this.keyValue = keyValue
    }
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
        val r = InternalRow(verticeIDs(currentSrcPosition), verticeIDs(edges(currentDstPosition).toInt))
        currentDstPosition += 1
        r
      }
    }
  }

  override def memoryUsage: Long = {
    verticeIDs.length * 8 + edgeIndices.length * 4 + edges.length * 8
  }

  // For testing
  def getVerticeIDs: Array[Long] = {
    verticeIDs
  }

  // For testing
  def getTranslatedEdges: Array[Long] = {
    edges.map(ei => verticeIDs(ei.toInt))
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
      val edgeIndicesSrcBuffer = new ArrayBuffer[Int](10000) // TODO those two are the same.
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
      (new CSRTrieIterable(Array[Long](), Array[Int](), Array[Long]()),
        new CSRTrieIterable(Array[Long](), Array[Int](), Array[Long]()))
    }
  }

  // For testing
  def buildBothDirectionsFrom(srcDst: Array[(Long, Long)], dstSrc: Array[(Long, Long)]): (CSRTrieIterable, CSRTrieIterable) = {
    buildBothDirectionsFrom(srcDst.map(t => new GenericInternalRow(Array[Any](t._1, t._2))).iterator,
      dstSrc.map(t => new GenericInternalRow(Array[Any](t._1, t._2))).iterator)
  }
}