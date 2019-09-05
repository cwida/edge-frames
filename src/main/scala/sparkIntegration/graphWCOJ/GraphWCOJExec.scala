package sparkIntegration.graphWCOJ

import java.util.concurrent.ConcurrentHashMap

import experiments.{Datasets, GraphWCOJ}
import experiments.metrics.Metrics
import leapfrogTriejoin.{CSRTrieIterable, TrieIterable}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
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

case class GraphWCOJExec(outputVariables: Seq[Attribute],
                         joinSpecification: JoinSpecification,
                         graphChild: SparkPlan,
                         partitionChild: SparkPlan) extends SparkPlan {
  private val logger = LoggerFactory.getLogger(classOf[WCOJExec])

  val JOIN_TIME_METRIC = "wcoj_join_time"
  val COPY_OUTPUT_TIME_METRIC = "copy_time"

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

  override protected def doExecute(): RDD[InternalRow] = {
    val config = WCOJConfiguration.get(sparkContext)

    val joinTime = longMetric(JOIN_TIME_METRIC)
    val copyTime = longMetric(COPY_OUTPUT_TIME_METRIC)

    val joinTimer = Metrics.getTimer(sparkContext, JOIN_TIME_METRIC)
    val copyTimer = Metrics.getTimer(sparkContext, COPY_OUTPUT_TIME_METRIC)

    var copyTimeAcc: Long = 0L
    var joinTimeAcc: Long = 0L

    val beforeTime = System.nanoTime()

    config.getJoinAlgorithm match {
      case experiments.WCOJ => {
        throw new UnsupportedOperationException("WCOJ distributed mode not supported yet")
      }
      case GraphWCOJ => {
        val partitionRDD = partitionChild.execute()
        val csrBroadcast = graphChild.executeBroadcast[(TrieIterable, TrieIterable)]()
        // TODO not correct yet and not query specific

        val ret = partitionRDD.mapPartitionsWithIndex((partition, _) => {
          val tc = TaskContext.get()

          joinSpecification.partitioning match {
            case p @ FirstVariablePartitioningWithWorkstealing(batchSize) => {
              // TODO use stage attempt as well for ID?
              val col = (0 to csrBroadcast.value._1.asInstanceOf[CSRTrieIterable].maxValue by batchSize)
              val id = FirstVariablePartitioningWithWorkstealing.newQueue(tc.stageId(), col)
              p.queueID = id
            }
            case _ => {
              /* NOP */
              -1
            }
          }

          // TODO empty partitions?
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
                false
              } else {
                val start = System.nanoTime()
                row = join.next()
                joinTimeAcc += (System.nanoTime() - start)
                true
              }
            }

            override def getRow: InternalRow = {
              val start = System.nanoTime()
              internalRowBuffer.row = row
              val ur = toUnsafeProjection(internalRowBuffer)
              copyTimeAcc += (System.nanoTime() - start)

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
