package sparkIntegration

import leapfrogTriejoin.CSRTrieIterable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.SparkPlan

case class ToCSRTrieIterableRDDExec(child: SparkPlan, attributeOrdering: Seq[String])
  extends ToTrieIterableRDDExec(child, attributeOrdering) {

  override protected def doExecute(): RDD[InternalRow] = {
    val matTime = longMetric(MATERIALIZATION_TIME_METRIC)
    val memoryUsage = longMetric(MEMORY_USAGE_METRIC)

    new TrieIterableRDD[CSRTrieIterable](child.execute()
      .mapPartitions(iter => {
        val start = System.nanoTime()
        val trieIterable = new CSRTrieIterable(iter.map(
          ir => {
            if (attributeOrdering == Seq("src", "dst")) {
              new GenericInternalRow(Array[Any](ir.getLong(0), ir.getLong(1)))  // TODO should I safe this rewrite, e.g. by doing it in ArrayTrieIterable
            } else {
              new GenericInternalRow(Array[Any](ir.getLong(1), ir.getLong(0)))
            }
          }
        ))

        matTime += (System.nanoTime() - start) / 1000000
        memoryUsage += trieIterable.memoryUsage

        Iterator(trieIterable)
      }))
  }

}
