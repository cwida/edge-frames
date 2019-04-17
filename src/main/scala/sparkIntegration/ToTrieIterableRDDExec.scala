package sparkIntegration

import leapfrogTriejoin.ArrayTrieIterable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetrics

case class ToTrieIterableRDDExec(child: SparkPlan) extends SparkPlan{
  val MATERIALIZATION_TIME_METRIC = "materializationTime"

  override lazy val metrics = Map(
    MATERIALIZATION_TIME_METRIC -> SQLMetrics.createTimingMetric(sparkContext, "materialization time"))

  override protected def doExecute(): RDD[InternalRow] = {
    val matTime = longMetric(MATERIALIZATION_TIME_METRIC)

    new TrieIterableRDD[ArrayTrieIterable](child.execute()
      .mapPartitions(iter => {
        val start = System.nanoTime()
        val ret = Iterator(new ArrayTrieIterable(iter))
        val end = System.nanoTime()
        matTime += (end - start) / 1000000
        ret
      }))
  }

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    val srcAtt = child.output.filter(att => att.name == "src").head  // TODO make src a constant
    val dstAtt = child.output.filter(att => att.name == "dst").head
    Seq(Seq(SortOrder(srcAtt, Ascending), SortOrder(dstAtt, Ascending)))
  }
}
