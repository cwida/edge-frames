package org.apache.spark.sql

import experiments.metrics.Metrics
import leapfrogTriejoin.{CSRTrieIterable, TrieIterable}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, SortOrder}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{SQLExecution, SparkPlan}
import org.apache.spark.util.ThreadUtils
import org.apache.spark.{SparkException, broadcast}
import sparkIntegration.WCOJConfiguration
import sparkIntegration.graphWCOJ.CSRCache

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future, TimeoutException}


// TODO should be exchange?
case class CSRTrieIterableBroadcast(graphID: Int, forwardEdges: SparkPlan, backwardEdges: SparkPlan) extends SparkPlan {
  private val broadcastTimeout = WCOJConfiguration.get(sparkContext).broadcastTimeout

  override val children: Seq[SparkPlan] = Seq(forwardEdges, backwardEdges)

  val MATERIALIZATION_TIME_METRIC = "materializationTime"
  val MEMORY_USAGE_METRIC = "memoryConsumption" // TODO repair memory size metric
  val COLLECT_TIME = "collectTime"
  val BUILD_TIME = "buildTime"
  val BROADCAST_TIME = "broadcastTime"

  override lazy val metrics: Map[String, SQLMetric] = Map(
    MATERIALIZATION_TIME_METRIC -> SQLMetrics.createTimingMetric(sparkContext, "materialization time"),
    MEMORY_USAGE_METRIC -> SQLMetrics.createSizeMetric(sparkContext, "materialized memory consumption"),
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    COLLECT_TIME -> SQLMetrics.createTimingMetric(sparkContext, "time to collect"),
    BUILD_TIME -> SQLMetrics.createTimingMetric(sparkContext, "time to build"),
    BROADCAST_TIME -> SQLMetrics.createTimingMetric(sparkContext, "time to broadcast")
  )

  override def output: Seq[Attribute] = {
    forwardEdges.output ++ backwardEdges.output.reverse
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    val columnNameToAttribute = (c: SparkPlan, n: String) => c.output.filter(att => att.name == n).head
    Seq(
      Seq("src", "dst").map(n => SortOrder(columnNameToAttribute(forwardEdges, n), Ascending)),
      Seq("dst", "src").map(n => SortOrder(columnNameToAttribute(backwardEdges, n), Ascending))
    )
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException("CSRTrieIterableBroadcast does not support the execute() code path.")
  }

  @transient
  private val timeout: Duration = {
    val timeoutValue = broadcastTimeout
    if (timeoutValue < 0) {
      Duration.Inf
    } else {
      timeoutValue.seconds
    }
  }

  @transient
  private lazy val relationFuture: Future[broadcast.Broadcast[(CSRTrieIterable, CSRTrieIterable)]] = {
    // broadcastFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    Future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(sqlContext.sparkSession, executionId) {
        val beforeCollect = System.nanoTime()

        // TODO can I build the CSR still in parallel on the executors?

        // Use executeCollect/executeCollectIterator to avoid conversion to Scala types
        val (sizeHintForward, forwardInput) = forwardEdges.executeCollectIterator()
        val (sizeHintBackwards, backwardInput) = backwardEdges.executeCollectIterator()

        val beforeBuild = System.nanoTime()
        longMetric(COLLECT_TIME) += (beforeBuild - beforeCollect) / 1000000

        val (csrForward, csrBackward) = CSRTrieIterable.buildBothDirectionsFrom(forwardInput,
          backwardInput.map(t => InternalRow(t.getLong(1), t.getLong(0))))

        val beforeBroadcast = System.nanoTime()
        longMetric(BUILD_TIME) += (beforeBroadcast - beforeBuild) / 1000000

        val broadcasted = sparkContext.broadcast((csrForward, csrBackward))
        val end = System.nanoTime()
        longMetric(BROADCAST_TIME) += (end - beforeBroadcast) / 1000000

        longMetric(MATERIALIZATION_TIME_METRIC) += (end - beforeCollect) / 1000000
        Metrics.masterTimers.put(MATERIALIZATION_TIME_METRIC, end - beforeCollect)

        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
        broadcasted
      }
    }(CSRTrieIterableBroadcast.executionContext)
  }

  override protected def doPrepare(): Unit = {
    relationFuture
  }

  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    try {
      val b = ThreadUtils.awaitResult(relationFuture, timeout).asInstanceOf[broadcast.Broadcast[T]]
      CSRCache.put(graphID, b.asInstanceOf[Broadcast[(TrieIterable, TrieIterable)]])
      b
    } catch {
      case ex: TimeoutException => {
        logError(s"Could not execute broadcast in ${timeout.toSeconds} secs.", ex)
        throw new SparkException(s"Could not execute broadcast in ${timeout.toSeconds} secs. " +
          s"You can increase the timeout for broadcasts via the WCOJConfiguration.",
          ex)
      }
    }
  }
}

object CSRTrieIterableBroadcast {
  val executionContext: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("wcoj-broadcast-exchange", 128))
}