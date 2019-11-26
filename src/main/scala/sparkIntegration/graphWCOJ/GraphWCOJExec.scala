package sparkIntegration.graphWCOJ

import java.util.concurrent.ConcurrentHashMap

import experiments.{Datasets, GraphWCOJ}
import experiments.metrics.Metrics
import leapfrogTriejoin.{CSRTrieIterable, TrieIterable}
import org.apache.spark.{BarrierTaskContext, TaskContext}
import org.apache.spark.rdd.{RDD, RDDBarrier}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BoundReference, UnsafeProjection}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{RowIterator, SparkPlan}
import org.apache.spark.util.SizeEstimator
import org.slf4j.LoggerFactory
import partitioning.FirstVariablePartitioningWithWorkstealing
import sparkIntegration.wcoj.WCOJExec
import sparkIntegration.{JoinSpecification, WCOJConfiguration, WCOJInternalRow}

import collection.JavaConverters._
import scala.collection.concurrent.Map
import scala.reflect.ClassTag

case class GraphWCOJExec(outputVariables: Seq[Attribute],
                         joinSpecification: JoinSpecification,
                         graphChild: SparkPlan,
                         partitionChild: SparkPlan) extends SparkPlan {
  private val logger = LoggerFactory.getLogger(classOf[WCOJExec])

  val JOIN_TIME_METRIC = "wcoj_join_time"
  val COPY_OUTPUT_TIME_METRIC = "copy_time"
  val BEFORE_AFTER_TIME = "algorithm_end"
  val UNTIL_SCHEDULED = "scheduled"

  override lazy val metrics = Map(
    JOIN_TIME_METRIC -> SQLMetrics.createTimingMetric(sparkContext, "wcoj time"),
    COPY_OUTPUT_TIME_METRIC -> SQLMetrics.createTimingMetric(sparkContext, "copy")
  )


  override def children: Seq[SparkPlan] = {
    Seq(graphChild, partitionChild)
  }

  override def output: Seq[Attribute] = {
    outputVariables
  }

  override def references: AttributeSet = {
    AttributeSet(Seq(graphChild).flatMap(c => c.output.filter(a => List("src", "dst").contains(a.name))))
  }

  private def calculateRoundRobinWorkstealingValues(partition: Int,
                                               barrierTaskContext: BarrierTaskContext,
                                               batchSize: Int,
                                               maxValue: Int) = {
    require(batchSize == 1)
    val taskInfos = barrierTaskContext.getTaskInfos()
    val ownTaskInfo = taskInfos(partition)

    val executors: Seq[String] = Set(taskInfos.map(_.address): _*).toSeq.sorted
    val numberOfExecutors = executors.size

    val currentExecutorIndex = executors.indexOf(ownTaskInfo.address)

    (currentExecutorIndex until maxValue by numberOfExecutors)
  }

  private def calculateRangeWorkstealingValues(partition: Int,
                                          barrierTaskContext: BarrierTaskContext,
                                          batchSize: Int,
                                          maxValue: Int): Seq[Int] = {
    val taskInfos = barrierTaskContext.getTaskInfos()
    val ownTaskInfo = taskInfos(partition)

    val numberOfTasks = taskInfos.length
    val valuesPerTask = maxValue / numberOfTasks

    val executors: Seq[String] = Set(taskInfos.map(_.address): _*).toSeq.sorted
    val numberOfExecutors = executors.size

    val currentExecutorIndex = executors.indexOf(ownTaskInfo.address)

    val tasksPerExecutor = taskInfos.map(ti => executors.indexOf(ti.address)).groupBy(identity)

    val numberOfTasksOnCurrentExecutor = tasksPerExecutor(currentExecutorIndex).length

    val tasksUpToCurrentExecutor: Int = if (currentExecutorIndex == 0) {
      0
    } else {
      (0 until currentExecutorIndex).map(i => tasksPerExecutor(i).length).sum
    }

    if (currentExecutorIndex == numberOfExecutors - 1) {
      (valuesPerTask * tasksUpToCurrentExecutor to maxValue by batchSize)
    } else {
      (valuesPerTask * tasksUpToCurrentExecutor
        until valuesPerTask * (tasksUpToCurrentExecutor + numberOfTasksOnCurrentExecutor) by batchSize)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val config = WCOJConfiguration.get(sparkContext)

    val joinTime = longMetric(JOIN_TIME_METRIC)
    val copyTime = longMetric(COPY_OUTPUT_TIME_METRIC)

    val joinTimer = Metrics.getTimer(sparkContext, JOIN_TIME_METRIC)
    val copyTimer = Metrics.getTimer(sparkContext, COPY_OUTPUT_TIME_METRIC)
    val algorithmEndTime = Metrics.getTimer(sparkContext, BEFORE_AFTER_TIME)
    val scheduledTime = Metrics.getTimer(sparkContext, UNTIL_SCHEDULED)

    val tasksPerWorker = Metrics.getTimer(sparkContext, "tasks")
    val partitionToExecutor = Metrics.getStringAccumable(sparkContext, "executors")

    var copyTimeAcc: Long = 0L
    var joinTimeAcc: Long = 0L

    Metrics.masterTimers.update("algorithmStart", System.currentTimeMillis())

    config.getJoinAlgorithm match {
      case experiments.WCOJ => {
        throw new UnsupportedOperationException("WCOJ distributed mode not supported yet")
      }
      case GraphWCOJ => {
        val partitionRDD = partitionChild.execute()
        val csrBroadcast = graphChild.executeBroadcast[(TrieIterable, TrieIterable)]()

        val maybeBarrierPartitionRDD = joinSpecification.partitioning match {
          case FirstVariablePartitioningWithWorkstealing(_) => {
            new MaybeBarrierRDD(Some(partitionRDD.barrier()), None)
          }
          case _ => new MaybeBarrierRDD(None, Some(partitionRDD))
        }

        val ret = maybeBarrierPartitionRDD.mapPartitions(_ => {
          val tc = TaskContext.get()
          val partition = tc.partitionId()

          val executor = joinSpecification.partitioning match {
            case FirstVariablePartitioningWithWorkstealing(_) => {
              val btc = BarrierTaskContext.get()
              val myAddress = btc.getTaskInfos()(partition).address
              myAddress + ";" + Thread.currentThread().getId
            }
            case _ => {
              Thread.currentThread().getId.toString
            }
          }
          partitionToExecutor.add(partition, executor)

          scheduledTime.add(partition, System.currentTimeMillis())

          joinSpecification.partitioning match {
            case p @ FirstVariablePartitioningWithWorkstealing(batchSize) => {
              val btc = BarrierTaskContext.get()
              // TODO use stage attempt as well for ID?

              val col = calculateRangeWorkstealingValues(partition, btc, batchSize, csrBroadcast.value._1.asInstanceOf[CSRTrieIterable].maxValue)
              val id = FirstVariablePartitioningWithWorkstealing.newQueue(tc.stageId(), col)

              p.queueID = id
            }
            case _ => {
              /* NOP */
              -1
            }
          }

          val toUnsafeProjection = UnsafeProjection.create(output.zipWithIndex.map({
            case (a, i) => {
              BoundReference(i, a.dataType, a.nullable)
            }
          }))

          val csr = csrBroadcast.value
          val join = joinSpecification.build(Seq(csr._1, csr._2), partition)

          val iter = new RowIterator {
            var row: Array[Long] = null
            val rowSize = joinSpecification.allVariables.size
            val internalRowBuffer = new WCOJInternalRow(new Array[Long](rowSize))

            override def advanceNext(): Boolean = {
              if (join.atEnd) {
                joinTime.set(joinTimeAcc / 1000000)
                copyTime.set(copyTimeAcc / 1000000)

                joinTimer.add(partition, joinTimeAcc)
                copyTimer.add(partition, copyTimeAcc)
                algorithmEndTime.add(partition, System.currentTimeMillis())

                tasksPerWorker.add(partition, join.getTasks)
                false
              } else {
//                val start = System.nanoTime()
                row = join.next()
//                joinTimeAcc += (System.nanoTime() - start)
                true
              }
            }

            override def getRow: InternalRow = {
//              val start = System.nanoTime()
              internalRowBuffer.row = row
              val ur = toUnsafeProjection(internalRowBuffer)
//              copyTimeAcc += (System.nanoTime() - start)

              ur
            }
          }
          iter.toScala
        }
        )
        ret
      }
    }
  }
}

class MaybeBarrierRDD[T](rddBarrier: Option[RDDBarrier[T]], rdd: Option[RDD[T]]) {
  def mapPartitions[S](f: (scala.Iterator[T]) => scala.Iterator[S])(implicit evidence: ClassTag[S]): org.apache.spark.rdd.RDD[S] = {
    if (rddBarrier.nonEmpty) {
      rddBarrier.get.mapPartitions(f)
    } else {
      rdd.get.mapPartitions(f)
    }
  }
}
