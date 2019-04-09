package scalaIntegration

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, GenericInternalRow}
import org.apache.spark.sql.execution.{RowIterator, SparkPlan}
import org.apache.spark.sql.types.IntegerType

case class WCOJExec(joinSpecification: JoinSpecification, child: SparkPlan) extends SparkPlan {
  override def children: Seq[SparkPlan] = child :: Nil

  override def output: Seq[Attribute] = {
    joinSpecification.allVariables.map(name => {
      val reference = if (joinSpecification.bindsOnFirstLevel(name))
        child.output.filter(a => a.name == "src").head
      else
        child.output.filter(a => a.name == "dst").head
      AttributeReference(name, IntegerType, false)(reference.exprId)
    })
  }
  override def references: AttributeSet = AttributeSet(child.output.filter(a => List("src", "dst").contains(a.name)))

  override protected def doExecute(): RDD[InternalRow] = {
    val childRDD = child.execute()
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
