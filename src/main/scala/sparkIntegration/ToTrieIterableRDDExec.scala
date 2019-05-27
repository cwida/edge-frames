package sparkIntegration

import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, SortOrder}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

abstract class ToTrieIterableRDDExec(child: SparkPlan, attributeOrdering: Seq[String]) extends UnaryExecNode {
  val MATERIALIZATION_TIME_METRIC = "materializationTime"
  val MEMORY_USAGE_METRIC = "memoryConsumption"

  override lazy val metrics = Map(
    MATERIALIZATION_TIME_METRIC -> SQLMetrics.createTimingMetric(sparkContext, "materialization time"),
    MEMORY_USAGE_METRIC -> SQLMetrics.createSizeMetric(sparkContext, "materialized memory consumption")
  )

  override def output: Seq[Attribute] = if (attributeOrdering == Seq("dst", "src")) { child.output.reverse } else { child.output }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    val columnNameToAttribute = (c: SparkPlan, n: String) => c.output.filter(att => att.name == n).head
    Seq(attributeOrdering.map(n => SortOrder(columnNameToAttribute(child, n), Ascending)))
  }
}
