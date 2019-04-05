package scalaIntegration

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.execution.{RowIterator, SparkPlan}

case class WCOJExec(joinSpecification: JoinSpecification, child: SparkPlan) extends SparkPlan {
  override protected def doExecute(): RDD[InternalRow] = {
    val childRDD = child.execute()
    val join = joinSpecification.build(childRDD)
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

      override def getRow: InternalRow = new GenericInternalRow(row.asInstanceOf[Array[Any]])
    }
    new RDD[InternalRow](childRDD) {
      override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
        iter.toScala
      }

      override protected def getPartitions: Array[Partition] = childRDD.partitions
    }
  }

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

}
