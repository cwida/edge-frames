package sparkIntegration

import leapfrogTriejoin.{ArrayTrieIterable, CSRTrieIterable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, GenericInternalRow, SortOrder}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class ToCSRTrieIterableRDDExec(children: Seq[SparkPlan]) extends SparkPlan {
  require(children.size == 2, "ToCSRTrieIterableRDDExec expects exactly two children")

  val MATERIALIZATION_TIME_METRIC = "materializationTime"
  val MEMORY_USAGE_METRIC = "memoryConsumption"

  override lazy val metrics: Map[String, SQLMetric] = Map(
    MATERIALIZATION_TIME_METRIC -> SQLMetrics.createTimingMetric(sparkContext, "materialization time"),
    MEMORY_USAGE_METRIC -> SQLMetrics.createSizeMetric(sparkContext, "materialized memory consumption")
  )

  override def output: Seq[Attribute] = children(0).output ++ children(1).output.reverse

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    val columnNameToAttribute = (c: SparkPlan, n: String) => c.output.filter(att => att.name == n).head
    Seq(
      Seq("src", "dst").map(n => SortOrder(columnNameToAttribute(children(0), n), Ascending)),
      Seq("dst", "src").map(n => SortOrder(columnNameToAttribute(children(1), n), Ascending))
    )
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val matTime = longMetric(MATERIALIZATION_TIME_METRIC)
    val memoryUsage = longMetric(MEMORY_USAGE_METRIC)

    new TwoTrieIterableRDD[CSRTrieIterable](children(0).execute().zipPartitions(children(1).execute())( { case (srcDstIter: Iterator[InternalRow], dstSrcIter: Iterator[InternalRow]) => {
        val start = System.nanoTime()
        val (srcDstCSR, dstSrcCSR) = CSRTrieIterable.buildBothDirectionsFrom(srcDstIter, dstSrcIter)

        matTime += (System.nanoTime() - start) / 1000000
        memoryUsage += srcDstCSR.memoryUsage
        memoryUsage += dstSrcCSR.memoryUsage

        Iterator((srcDstCSR, dstSrcCSR))
      }}))
  }
}
