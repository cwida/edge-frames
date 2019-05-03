package leapfrogTriejoin

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import collection.JavaConverters._


class ArrayTrieIterable(iter: Iterator[InternalRow]) extends TrieIterable {
    // TODO capacity optimization
  private val srcColumn = new OnHeapColumnVector(1000, IntegerType)
  private val dstColumn = new OnHeapColumnVector(1000, IntegerType)
  private var numRows = 0

  while (iter.hasNext) {
    val row = iter.next()
    srcColumn.appendInt(row.getInt(0))
    dstColumn.appendInt(row.getInt(1)) // TODO sync field names and position
    numRows += 1
  }
  private val tuples = new ColumnarBatch(Array(srcColumn, dstColumn))
  tuples.setNumRows(numRows)

  // For testing
  def this(a: Array[(Int, Int)]) {
    this(a.map(t => new GenericInternalRow(Array[Any](t._1, t._2))).iterator)
  }

  override def trieIterator: TrieIterator = {
    new TrieIteratorImpl(tuples)
  }

  def getMemoryUsage(): Long = {
    numRows * 2 * 4
  }

  class TrieIteratorImpl(val tuples: ColumnarBatch) extends TrieIterator {
    private val maxDepth = tuples.numCols() - 1
    private val numRows = tuples.numRows()

    private var depth = -1
    private var position = Array.fill(tuples.numCols())(-1)
    private var end = Array.fill(tuples.numCols())(-1)
    private var isAtEnd = numRows == 0

    private var currentColumn: ColumnVector = null
    private var currentPosition: Int = -1

    override def open(): Unit = {
      assert(depth < maxDepth, "Cannot open TrieIterator at maxDepth")

      var newEnd = numRows
      if (depth >= 0) {  // TODO remove ifs?
        newEnd = currentPosition

        val beginValue = currentColumn.getInt(currentPosition)
        do {
          newEnd += 1
        } while (newEnd + 1 <= numRows
          && currentColumn.getInt(newEnd) == beginValue)

        position(depth) = currentPosition
      }

      depth += 1

      end(depth) = newEnd
      currentColumn = tuples.column(depth)
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
        currentColumn = tuples.column(depth)
      }
      isAtEnd = false
    }

    override def key: Int = {
      assert(!atEnd, "Calling key on TrieIterator atEnd is illegal.")
      currentColumn.getInt(currentPosition)
    }

    override def next(): Unit = {
      assert(numRows > currentPosition, "No next value, check atEnd before calling next")
      seek(currentColumn.getInt(currentPosition) + 1)

      // TODO can I optimize here?
    }

    override def atEnd: Boolean = {
      isAtEnd
    }

    override def seek(key: Int): Unit = {
      currentPosition = GallopingSearch.find(currentColumn, key, currentPosition, end(depth))
      updateAtEnd()
    }

    private def updateAtEnd() {
      if (currentPosition >= numRows) {
        isAtEnd = true
      } else if (depth != 0
        && tuples.column(depth - 1).getInt(position(depth - 1)) != tuples.column(depth - 1).getInt(currentPosition)) {
        isAtEnd = true
      }
    }
  }

  override def iterator: Iterator[InternalRow] = {
    tuples.rowIterator().asScala
  }
}
