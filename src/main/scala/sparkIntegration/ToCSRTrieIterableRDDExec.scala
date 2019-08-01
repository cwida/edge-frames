package org.apache.spark.sql

import leapfrogTriejoin.{ArrayTrieIterable, CSRTrieIterable, TrieIterable}
import org.apache.spark.{SparkException, broadcast}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, GenericInternalRow, SortOrder}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{SQLExecution, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.ThreadUtils
import sparkIntegration.{CSRCache, TwoTrieIterableRDD}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, TimeoutException}

case class ToCSRTrieIterableRDDExec(children: Seq[SparkPlan], graphID: Int) extends SparkPlan {
  require(children.size == 2, "ToCSRTrieIterableRDDExec expects exactly two children")

  val MATERIALIZATION_TIME_METRIC = "materializationTime"
  val MEMORY_USAGE_METRIC = "memoryConsumption"

  override lazy val metrics: Map[String, SQLMetric] = Map(
    MATERIALIZATION_TIME_METRIC -> SQLMetrics.createTimingMetric(sparkContext, "materialization time"),
    MEMORY_USAGE_METRIC -> SQLMetrics.createSizeMetric(sparkContext, "materialized memory consumption")
  )

  override def output: Seq[Attribute] = {
    children(0).output ++ children(1).output.reverse
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    val columnNameToAttribute = (c: SparkPlan, n: String) => c.output.filter(att => att.name == n).head
    Seq(
      Seq("src", "dst").map(n => SortOrder(columnNameToAttribute(children(0), n), Ascending)),
      Seq("dst", "src").map(n => SortOrder(columnNameToAttribute(children(1), n), Ascending))
    )
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "BroadcastExchange does not support the execute() code path.")

    val matTime = longMetric(MATERIALIZATION_TIME_METRIC)
    val memoryUsage = longMetric(MEMORY_USAGE_METRIC)

    new TwoTrieIterableRDD[CSRTrieIterable](children(0).execute().zipPartitions(children(1).execute())({ case (srcDstIter: Iterator[InternalRow], dstSrcIter: Iterator[InternalRow]) => {
      val start = System.nanoTime()
      val (srcDstCSR, dstSrcCSR) = CSRTrieIterable.buildBothDirectionsFrom(srcDstIter, dstSrcIter.map(t => InternalRow(t.getLong(1), t
        .getLong(0)))) // TODO I don't actually need the order exchange if I rewrite the build code a bit

      matTime += (System.nanoTime() - start) / 1000000
      memoryUsage += srcDstCSR.memoryUsage
      memoryUsage += dstSrcCSR.memoryUsage

      Iterator((srcDstCSR, dstSrcCSR))
    }
    }))
  }

  @transient
  private val timeout: Duration = {

    val timeoutValue = -1 // TODO make configurable? or use default from sqlContext?
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
        // Use executeCollect/executeCollectIterator to avoid conversion to Scala types
        // TODO can I build the CSR still in parallel on the executors?

        val forwardInput = children(0).executeCollect().toIterator
        val backwardInput = children(1).executeCollect().toIterator // TODO converts everything to scala types innefficient

        val beforeBuild = System.nanoTime()
//        longMetric("collectTime") += (beforeBuild - beforeCollect) / 1000000

        // Construct the relation.
        val (csrForward, csrBackward) = CSRTrieIterable.buildBothDirectionsFrom(forwardInput, backwardInput.map(t => InternalRow(t
          .getLong(1), t.getLong(0))))

        val beforeBroadcast = System.nanoTime()
//        longMetric("buildTime") += (beforeBroadcast - beforeBuild) / 1000000

        // Broadcast the relation
        val broadcasted = sparkContext.broadcast((csrForward, csrBackward))
//        longMetric("broadcastTime") += (System.nanoTime() - beforeBroadcast) / 1000000

        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
        broadcasted

      }
    }(ToCSRTrieIterableRDDExec.executionContext)
  }

  override protected def doPrepare(): Unit = {
    // Materialize the future.
    relationFuture
  }

  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {  // TODO can only be
    // used from within
    // package
    // sql...
    try {
      val b = ThreadUtils.awaitResult(relationFuture, timeout).asInstanceOf[broadcast.Broadcast[T]]
      CSRCache.put(graphID, b.asInstanceOf[Broadcast[(TrieIterable, TrieIterable)]])
      b
    } catch {
      case ex: TimeoutException => {
        logError(s"Could not execute broadcast in ${timeout.toSeconds} secs.", ex)
        throw new SparkException(s"Could not execute broadcast in ${timeout.toSeconds} secs. " +
          s"You can increase the timeout for broadcasts via ${SQLConf.BROADCAST_TIMEOUT.key} or " +
          s"disable broadcast join by setting ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1",
          ex)
      }
    }
  }
}

object ToCSRTrieIterableRDDExec {
   val executionContext = ExecutionContext.fromExecutorService(  // TODO do I want a public thread utils?
    ThreadUtils.newDaemonCachedThreadPool("broadcast-exchange", 128))
}