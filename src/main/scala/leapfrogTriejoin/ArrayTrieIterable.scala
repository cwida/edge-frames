package leapfrogTriejoin

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class ArrayTrieIterable() extends TrieIterable {
  private var tuples: Array[(Int, Int)] = null
  private var tuples1: ColumnarBatch = null

  def this(iter: Iterator[(Int, Int)]) {
    this()
    tuples = iter.toArray
  }

  def this(iter: Iterator[InternalRow], columnar: Boolean) {
    this()
    // TODO capacity optimization
    val srcColumn = new OnHeapColumnVector(1000, IntegerType)
    val dstColumn = new OnHeapColumnVector(1000, IntegerType)

    while(iter.hasNext) {
      val row = iter.next()
      srcColumn.appendInt(row.getInt(0))
      dstColumn.appendInt(row.getInt(1))  // TODO sync field names and position
    }
    tuples1 = new ColumnarBatch(Array(srcColumn, dstColumn))
  }

  def trieIterator: TrieIterator = {
    new TreeTrieIterator(tuples)  // TODO change to real implementation
  }

  class TrieIteratorImpl(val tuples: ColumnarBatch) extends TrieIterator {
    private val maxDepth = tuples.numCols() - 1

    private var depth = -1
    private var position = Array.fill(tuples.numCols())(-1)
    private var isAtEnd = false

    override def open(): Unit = {
      assert(depth < maxDepth, "Cannot open TrieIterator at maxDepth")
      depth += 1
      position(depth) = position(depth - 1)
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
      tuples.column(depth).getInt(position(depth))
    }

    override def next(): Unit = {
      assert(tuples.numRows() > position(depth), "No next value, check atEnd before calling next")
      seek(tuples.column(depth).getInt(position(depth)) + 1)
    }

    override def atEnd: Boolean = atEnd

    override def seek(key: Int): Unit = {
      position(depth) = GaloppingSearch.find(tuples.column(depth), key, position(depth), tuples.numRows())
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

  override def iterator: Iterator[(Int, Int)] = {
    tuples.iterator
  }
}
