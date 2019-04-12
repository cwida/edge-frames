package sparkIntegration

import leapfrogTriejoin.ArrayTrieIterable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan

case class ToTrieIterableRDDExec(child: SparkPlan) extends SparkPlan{
  override protected def doExecute(): RDD[InternalRow] = {
    new TrieIterableRDD[ArrayTrieIterable](child.execute()
      .mapPartitions(iter => Iterator(new ArrayTrieIterable(iter.map(ir => (ir.getInt(0), ir.getInt(1)))))))
  }

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil
}
