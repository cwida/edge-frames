package sparkIntegration

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, GenericInternalRow}
import org.apache.spark.sql.execution.{RowIterator, SparkPlan}
import org.apache.spark.sql.types.{IntegerType, LongType}

case class WCOJExec(joinSpecification: JoinSpecification, children: Seq[SparkPlan]) extends SparkPlan {

  override def output: Seq[Attribute] = {
    joinSpecification.allVariables.map(v => {
      val relI = joinSpecification.variableToRelationshipIndex(v)
      val aI = joinSpecification.variableToAttributeIndex(v)
      val ref = children(relI).output.filter(a => a.name == (if (aI == 0) "src" else "dst")).head
      new AttributeReference(v, IntegerType, false)(ref.exprId)
    })
  }

  override def references: AttributeSet = AttributeSet(children.flatMap(c => c.output.filter(a => List("src", "dst").contains(a.name))))

  override protected def doExecute(): RDD[InternalRow] = {

    // TODO mehtod copies all tuples from an RDD into an array -- not good. Should initialize the TrieIterator from RDD's instead.
    val childRDD = children(0).execute()  // TODO not consistent with operator, assumes only one child or all children to be the same.
    childRDD.mapPartitions(rowIter => {
      val tuples = rowIter.map(ir => (ir.getInt(0), ir.getInt(1))).toArray

      val join = joinSpecification.build(tuples)
      val iter = new RowIterator {
        var row: Array[Int]= null
        override def advanceNext(): Boolean = {
          if (join.atEnd) {
            false
          } else {
            row = join.next()
            true
          }
        }

        override def getRow: InternalRow = {
          val gr = new GenericInternalRow(row.size)
          row.zipWithIndex.foreach { case(b, i) => gr.update(i.toInt, b) }
          gr
        }
      }
      iter.toScala
    })
  }
}
