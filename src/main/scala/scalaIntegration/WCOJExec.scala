package scalaIntegration

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.execution.{RowIterator, SparkPlan}

case class WCOJExec(joinSpecification: JoinSpecification, child: SparkPlan) extends SparkPlan {
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

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

}
