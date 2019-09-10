package sparkIntegration.wcoj

import experiments.metrics.Metrics
import leapfrogTriejoin.ArrayTrieIterable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, GenericInternalRow, SortOrder}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class ToArrayTrieIterableRDDExec(child: SparkPlan, attributeOrdering: Seq[String])
  extends UnaryExecNode {
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

  override protected def doExecute(): RDD[InternalRow] = {
    val matTime = longMetric(MATERIALIZATION_TIME_METRIC)
    val memoryUsage = longMetric(MEMORY_USAGE_METRIC)

    val materializationTimer = Metrics.getTimer(sparkContext, MATERIALIZATION_TIME_METRIC)

    new TrieIterableRDD[ArrayTrieIterable](child.execute()
      .mapPartitions(iter => {
        val start = System.currentTimeMillis()
        val trieIterable = new ArrayTrieIterable(iter.map(
          ir => {
            if (attributeOrdering == Seq("src", "dst")) {
              new GenericInternalRow(Array[Any](ir.getLong(0), ir.getLong(1)))
            } else {
              new GenericInternalRow(Array[Any](ir.getLong(1), ir.getLong(0)))
            }
          }
        ))

        val end = System.currentTimeMillis()

        matTime += (end - start) / 1000000
        materializationTimer.add((0, end - start))
        memoryUsage += trieIterable.memoryUsage

        Iterator(trieIterable)
      }))
  }
}
