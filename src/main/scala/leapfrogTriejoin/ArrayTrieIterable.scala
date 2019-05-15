package leapfrogTriejoin

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import collection.JavaConverters._


class ArrayTrieIterable(iter: Iterator[InternalRow]) extends TrieIterable {
  // TODO build needs to tbe done correclty
  private[this] val srcColumn = new OpenArrayColumnVector(4000000)
  private[this] val dstColumn = new OpenArrayColumnVector(4000000)
  private[this] var numRows = 0

  while (iter.hasNext) {
    val row = iter.next()
    srcColumn.appendLong(row.getLong(0))
    dstColumn.appendLong(row.getLong(1)) // TODO sync field names and position
    numRows += 1
  }
  private[this] val tuples = new ColumnarBatch(Array(srcColumn, dstColumn))
  tuples.setNumRows(numRows)

  // For testing
  def this(a: Array[(Long, Long)]) {
    this(a.map(t => new GenericInternalRow(Array[Any](t._1, t._2))).iterator)
  }

  override def trieIterator: TrieIterator = {
    new TrieIteratorImpl(tuples)
  }

  def getMemoryUsage(): Long = {
    numRows * 2 * 4
  }

  class TrieIteratorImpl(val tuples: ColumnarBatch) extends TrieIterator {
    private[this] val maxDepth = tuples.numCols() - 1
    private[this] val numRows = tuples.numRows()

    private[this] var depth = -1
    private[this] var position = Array.fill(tuples.numCols())(-1)
    private[this] var end = Array.fill(tuples.numCols())(-1)
    private[this] var isAtEnd = numRows == 0

    private[this] val columns = Array(tuples.column(0).asInstanceOf[OpenArrayColumnVector].longData, tuples.column(1).asInstanceOf[OpenArrayColumnVector].longData)
    private[this] var currentColumn: Array[Long] = null
    private[this] var currentPosition: Int = -1
    private[this] var currentEnd: Int = -1

    override def open(): Unit = {
      assert(depth < maxDepth, "Cannot open TrieIterator at maxDepth")

      var newEnd = numRows
      if (depth >= 0) { // TODO remove ifs?
        newEnd = currentPosition

        val beginValue = currentColumn(currentPosition)
        do {
          newEnd += 1
        } while (newEnd + 1 <= numRows
          && currentColumn(newEnd) == beginValue)

        position(depth) = currentPosition
      }

      depth += 1

      end(depth) = newEnd
      currentEnd = newEnd
      currentColumn = columns(depth)
      isAtEnd = false

      currentPosition = if (depth != 0) {
        position(depth - 1)
      } else {
        0
      }
    }

    override def up(): Unit = {
      assert(-1 <= depth, "Cannot up TrieIterator at root level")

      depth -= 1
      if (depth >= 0) {
        currentPosition = position(depth)
        currentColumn = columns(depth)
        currentEnd = end(depth)
      }
      isAtEnd = false
    }

    override def key: Long = {
      assert(!atEnd, "Calling key on TrieIterator atEnd is illegal.")
      currentColumn(currentPosition)
    }

    override def next(): Unit = {
      assert(numRows > currentPosition, "No next value, check atEnd before calling next")
      if (depth == maxDepth) { // If we are at the last level column values are guaranteed to be unique because edges are unique.
        currentPosition += 1
        updateAtEnd()
      } else {
        // This is not the next value in the array but the next value in the array different from the current one
        seek(currentColumn(currentPosition) + 1) // Replacing this with a linear search is not a performance improvement
      }
    }

    override def atEnd: Boolean = {
      isAtEnd
    }

    override def seek(key: Long): Boolean = {
      if (key != this.key) {
        currentPosition = ArraySearch.find(currentColumn, key, currentPosition, end(depth))
        updateAtEnd()
      }
      isAtEnd
    }

    @inline
    private def updateAtEnd(): Unit = {
      isAtEnd = currentPosition >= currentEnd
    }
  }

  override def iterator: Iterator[InternalRow] = {
    tuples.rowIterator().asScala
  }
}
