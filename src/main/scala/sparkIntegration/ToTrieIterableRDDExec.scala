package sparkIntegration

import leapfrogTriejoin.ArrayTrieIterable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, SortOrder}
import org.apache.spark.sql.execution.SparkPlan

case class ToTrieIterableRDDExec(child: SparkPlan) extends SparkPlan{
  override protected def doExecute(): RDD[InternalRow] = {
    new TrieIterableRDD[ArrayTrieIterable](child.execute()
      .mapPartitions(iter => Iterator(new ArrayTrieIterable(iter))))
  }

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    val srcAtt = child.output.filter(att => att.name == "src").head  // TODO make src a constant
    val dstAtt = child.output.filter(att => att.name == "dst").head
    Seq(Seq(SortOrder(srcAtt, Ascending), SortOrder(dstAtt, Ascending)))
  }
}
