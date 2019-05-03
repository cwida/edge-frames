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

    private var depth = -1
    private var position = Array.fill(tuples.numCols())(-1)
    private var end = Array.fill(tuples.numCols())(-1)
    private var isAtEnd = tuples.numRows() == 0

    private var currentColumn: ColumnVector = null
//    private var currentPosition

    override def open(): Unit = {
      assert(depth < maxDepth, "Cannot open TrieIterator at maxDepth")

      var newEnd = tuples.numRows()
      if (depth >= 0) {
        newEnd = position(depth)
        do {
          newEnd += 1
        } while (newEnd + 1 <= tuples.numRows() && tuples.column(depth).getInt(newEnd) == tuples.column(depth).getInt(position(depth)))
      }

      depth += 1
      end(depth) = newEnd
      position(depth) = if (depth != 0) {
        position(depth - 1)
      } else {
        0
      }
      isAtEnd = false
    }

    override def up(): Unit = {
      assert(-1 <= depth, "Cannot up TrieIterator at root level")
      position(depth) = -1
      depth -= 1
      isAtEnd = false
    }

    override def key: Int = {
      assert(!atEnd, "Calling key on TrieIterator atEnd is illegal.")

      // TODO use column reference and position
      tuples.column(depth).getInt(position(depth))
    }

    override def next(): Unit = {
      assert(tuples.numRows() > position(depth), "No next value, check atEnd before calling next")
      seek(tuples.column(depth).getInt(position(depth)) + 1)
    }

    override def atEnd: Boolean = {
      isAtEnd
    }

    override def seek(key: Int): Unit = {
      position(depth) = GallopingSearch.find(tuples.column(depth), key, position(depth), end(depth))
      updateAtEnd()
    }

    private def updateAtEnd() {
      if (position(depth) >= tuples.numRows()) {
        isAtEnd = true
      } else if (depth != 0 && tuples.column(depth - 1).getInt(position(depth - 1)) != tuples.column(depth - 1).getInt(position(depth))) {
        isAtEnd = true
      }
    }
  }

  override def iterator: Iterator[InternalRow] = {
    tuples.rowIterator().asScala
  }
}
