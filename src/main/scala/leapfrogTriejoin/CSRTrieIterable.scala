package leapfrogTriejoin

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import partitioning.{Partitioning, RoundRobin, SharesRange}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// TODO serializing could be improved by sending common parts of the array only once
class CSRTrieIterable(private[this] val verticeIDs: Array[Long],
                      val edgeIndices: Array[Int],
                      private[this] val edges: Array[Long]) extends TrieIterable with Serializable {

  override def trieIterator: TrieIteratorImpl = {
    trieIterator(None, None, None, None, None)
  }

  def trieIterator(partition: Int,
                   partitions: Int,
                   partitioning: Partitioning,
                   dimensionFirstLevel: Int,
                   dimensionSecondLevel: Int): TrieIteratorImpl = {
    new TrieIteratorImpl(Option(partition), Option(partitions), Option(partitioning), Option(dimensionFirstLevel), Option
    (dimensionSecondLevel))
  }


  def trieIterator(partition: Option[Int],
                   partitions: Option[Int],
                   partitioning: Option[Partitioning],
                   dimensionFirstLevel: Option[Int],
                   dimensionSecondLevel: Option[Int]): TrieIteratorImpl = {
    new TrieIteratorImpl(partition, partitions, partitioning, dimensionFirstLevel, dimensionSecondLevel)
  }

  // TODO sort out range filtering functionality, either in here or in MultiRangePartitionTrieIterator
  class TrieIteratorImpl(
                          val partition: Option[Int],
                          val partitions: Option[Int],
                          val partitioning: Option[Partitioning],
                          val dimensionFirstLevel: Option[Int],
                          val dimensionSecondLevel: Option[Int]
                        ) extends TrieIterator {

    /*
     Partitioning
     Allows to bind both levels of the iterator to a range of values. The lower bound is included the upper bound is excluded.
     For no range partitionings, we choose 0 and the last node index + 1 as range, which has no effect because this is the original
     lower and upper bound.
     */
    private[this] val (firstLevelLowerBound, firstLevelUpperBound, secondLevelLowerBound, secondLevelUpperBound) = partitioning
    match {
//      case Some(SharesRange(Some(hypercube), _)) => {
//        val upper = edgeIndices.length - 1
//        val coordinate = hypercube.getCoordinate(partition.get)
//        val (fl, fu) = if (dimensionFirstLevel.get == 1) {
//          getPartitionBoundsInRange(0, upper, coordinate(dimensionFirstLevel.get), hypercube.dimensionSizes(dimensionFirstLevel.get),
//            true)
//        } else {
//          getPartitionBoundsInRange(0, upper, coordinate(dimensionFirstLevel.get), hypercube.dimensionSizes(dimensionFirstLevel.get),
//            false)
//        }
//        val (sl, su) = if (dimensionSecondLevel.get == 1) {
//          getPartitionBoundsInRange(0, upper, coordinate(dimensionSecondLevel.get), hypercube.dimensionSizes(dimensionSecondLevel.get),
//            true)
//        } else {
//          getPartitionBoundsInRange(0, upper, coordinate(dimensionSecondLevel.get), hypercube.dimensionSizes(dimensionSecondLevel.get),
//            false)
//        }
//        (fl, fu, sl, su)
//      }
      case _ => {
        (0, edgeIndices.length - 1, 0, edgeIndices.length - 1)
      }
    }

    private[this] val roundRobinNumber: Int = partitioning match {
      case Some(RoundRobin(v)) => if (v == dimensionFirstLevel.get) partitions.get else 1
      case _ => 1
    }


    private[this] var isAtEnd = verticeIDs.length == 0

    private[this] var depth = -1

    private[this] var srcPosition: Int = partitioning match {
      case Some(RoundRobin(v)) => if (v == dimensionFirstLevel.get) firstLevelLowerBound + partition.get else 1
      case _ => firstLevelLowerBound
    }

    if (!isAtEnd && edgeIndices(srcPosition) == edgeIndices(srcPosition + 1)) {
      moveToNextSrcPosition()
    }
    isAtEnd = firstLevelUpperBound <= srcPosition

    private[this] val firstSourcePosition = srcPosition

    private[this] var dstPosition = 0

    private[this] var keyValue = 0L

//    println("Init", partition.get)

    private def getPartitionBoundsInRange(lower: Int, upper: Int, partition: Int, numPartitions: Int, fromBelow: Boolean): (Int, Int) = {
      val totalSize = upper - lower
      val partitionSize = totalSize / numPartitions
      val lowerBound = if (fromBelow) {
        lower + partition * partitionSize
      } else {
        if (partition == numPartitions - 1) {
           lower
        } else {
          upper - (partition + 1) * partitionSize
        }
      }


      val upperBound = if (fromBelow) {
        if (partition == numPartitions - 1) {
          upper
        } else {
          lower + (partition + 1) * partitionSize
        }
      } else {
        upper - partition * partitionSize
      }
      (lowerBound, upperBound)
    }

    override def open(): Unit = {
      if (roundRobinNumber != 1 && depth == -1) {
//        println("Open", partition.get)
      }
      assert(!isAtEnd, "open cannot be called when atEnd")
      depth += 1

      if (depth == 0) {
        srcPosition = firstSourcePosition
        keyValue = srcPosition.toLong
      } else if (depth == 1) { // TODO predicatable
        dstPosition = edgeIndices(srcPosition)
        isAtEnd = secondLevelUpperBound <= edges(dstPosition)
        if (!isAtEnd && secondLevelLowerBound != 0) {
          seek(secondLevelLowerBound)
        } else {
          keyValue = edges(dstPosition)
        }
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
        isAtEnd = firstLevelUpperBound <= srcPosition
        keyValue = srcPosition.toLong
        if (roundRobinNumber != 1) {
//          println("Partition next", partition.get, keyValue)
        }
      } else {
        dstPosition += 1
        isAtEnd = dstPosition == edgeIndices(srcPosition + 1) || secondLevelUpperBound <= edges(dstPosition)
        // edgeIndices(srcPosition + 1) should not be factored out, it does not look like this improves performance (looks!)
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
      if (keyValue < key) {  // TODO quickfix, why does this call even happen, clique3 either amazon0302 or liveJournal
        assert(keyValue < key)
        if (depth == 0) {
          srcPosition = if (roundRobinNumber != 1 && key % roundRobinNumber != partition.get) {
            (key + key % roundRobinNumber + partition.get).toInt
          } else {
            key.toInt
          }
          val correctedKey = srcPosition

          if (srcPosition < edgeIndices.length - 1 && // TODO srcPosition should never be bigger than edgeIndices.lenght -  1, investigate
            edgeIndices(srcPosition) == edgeIndices(srcPosition + 1)) { // TODO does srcPosition < edgeIndices.length - 1 ruin
            // predicatability?
            moveToNextSrcPosition()
          }
          isAtEnd = firstLevelUpperBound <= srcPosition
          keyValue = srcPosition.toLong
          if (roundRobinNumber != 1) {

//            println("Partition seek", partition.get, keyValue, key, correctedKey)
          }
          isAtEnd
        } else {
          dstPosition = ArraySearch.find(edges, key, dstPosition, edgeIndices(srcPosition + 1))
          isAtEnd = dstPosition == edgeIndices(srcPosition + 1) || secondLevelUpperBound <= edges(dstPosition)
          if (!isAtEnd) {
            keyValue = edges(dstPosition)
          }
        }
      }
      isAtEnd
    }

    private def moveToNextSrcPosition(): Unit = {
      if (srcPosition + roundRobinNumber < edgeIndices.length) {
        var indexToSearch = edgeIndices(srcPosition + roundRobinNumber) // A linear search is ideal, see log 05.06

        do {
          srcPosition += roundRobinNumber
        } while (srcPosition < edgeIndices.length - 1 && edgeIndices(srcPosition + 1) == indexToSearch) // TODO sentry element
      } else {
        srcPosition += roundRobinNumber
      }

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
      val c = new TrieIteratorImpl(partition, partitions, partitioning, dimensionFirstLevel, dimensionSecondLevel)
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

  def minValue: Int = {
    0
  }

  def maxValue: Int = {
    edgeIndices.length - 1
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