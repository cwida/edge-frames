package partitioning

import java.util.concurrent.ConcurrentLinkedQueue

import org.slf4j.LoggerFactory
import partitioning.shares.Hypercube

import scala.collection.mutable
import scala.util.Random
import math.Ordering
import scala.annotation.elidable

import collection.JavaConverters._

sealed trait Partitioning {
  def getWorkersUsed(workersTotal: Int): Int
}

case class Shares(hypercube: Hypercube = Hypercube(Array[Int]())) extends Partitioning {
  override def toString: String = {
    if (hypercube.dimensionSizes.isEmpty) {
      "Shares(Uninitialized)"
    } else {
      s"Shares(${hypercube.dimensionSizes.mkString("; ")})"
    }
  }

  override def getWorkersUsed(workersTotal: Int): Int = {
    hypercube.dimensionSizes.product
  }
}

case class SharesRange(hypercube: Option[Hypercube] = None, prefix: Option[Int] = None) extends Partitioning {

  var rangesOpt: Option[Map[(Int, Int), Seq[(Int, Int)]]] = None

  override def toString: String = {
    hypercube match {
      case Some(Hypercube(dimensionSizes)) => {
        s"SharesRange(${dimensionSizes.mkString("; ")})"
      }
      case None => {
        s"SharesRange(Uninitialized, prefix=$prefix)"
      }
    }
  }

  override def getWorkersUsed(workersTotal: Int): Int = {
    hypercube.get.dimensionSizes.product
  }

  /**
    * Given the partition, dimension, the highest and lowest value in this dimension returns multiple assigned ranges.
    *
    * @return Sequence of ranges, lower bound inclusive, upper bound exclusive, none overlapping, sorted in increasing order.
    */
  def getRanges(partition: Int, dimension: Int, lower: Int, upper: Int): Seq[(Int, Int)] = {
    val coordinate = hypercube.get.getCoordinate(partition)

    val numberOfRanges = hypercube.get.dimensionSizes.product

    rangesOpt.getOrElse(computeRanges(lower, upper))((partition, dimension))
  }

  private def computeRanges(lower: Int, upper: Int): Map[(Int, Int), Seq[(Int, Int)]] = {
    val rangeMap = new mutable.HashMap[(Int, Int), Seq[(Int, Int)]]()

    val numberOfPartitions = hypercube.get.dimensionSizes.product

    val numberOfRanges = hypercube.get.dimensionSizes.product
    val rangeIndices = (0 until numberOfRanges).indices
    val ranges = rangeIndices.map(i => getPartitionBoundsInRange(lower, upper, i, numberOfRanges, fromBelow = true))

    val otherRangeMap = new mutable.HashMap[(Int, Int), Seq[(Int, Int)]]()
    // Ranges first dimension
    val firstDimensionSize = hypercube.get.dimensionSizes.head
    val rangesToAssign = numberOfRanges / firstDimensionSize
    for (c <- 0 until firstDimensionSize) {
      otherRangeMap((0, c)) = ranges.slice(c * rangesToAssign, (c+ 1) * rangesToAssign )
    }


    // Ranges with overlap first dimension
    val secondDimesionSize = hypercube.get.dimensionSizes(1)
    val rangesToAssignWithOverlap = numberOfRanges / (firstDimensionSize * secondDimesionSize)
    for (c1 <- 0 until secondDimesionSize) {
      for (c0 <- 0 until firstDimensionSize) {
        otherRangeMap((1, c1)) =
          otherRangeMap((0, c0)).slice(c1 * rangesToAssignWithOverlap, (c1 + 1) * rangesToAssignWithOverlap)
      }
    }

    val rangesForDimesion = otherRangeMap.filter(_._1._1 == 1).values.flatten.toSeq
    require(rangesForDimesion.size == rangesForDimesion.toSet.size, s"Dimension 1 is overlapping, ${rangesForDimesion.mkString(", ")}")

    // Ranges without overlap first dimension
    val takenRanges: Set[(Int, Int)] = otherRangeMap.filter({
      case ((d, _), _) => {
        d == 1
      }
    }).values.flatten.toSet
    val freeRanges = ranges.filter(r => !takenRanges.contains(r)).toSeq

    val rangesToAssigWithoutOverlap = numberOfRanges / secondDimesionSize - rangesToAssignWithOverlap
    for (c1 <- 0 until secondDimesionSize) {
        otherRangeMap((1, c1)) = (otherRangeMap((1, c1)) ++
          freeRanges.slice(c1 * rangesToAssigWithoutOverlap, (c1 + 1) * rangesToAssigWithoutOverlap)).sorted
    }

    // Other dimensions
    val random = new Random(2)
    for (d <- 2 until hypercube.get.dimensionSizes.length) {
      val dimensionSize = hypercube.get.dimensionSizes(d)
      val freeRanges = random.shuffle(Seq(ranges: _*))
      val rangesToAssign = numberOfRanges / dimensionSize
      for (c <- 0 until dimensionSize) {
        otherRangeMap((d, c)) = freeRanges.slice(c * rangesToAssign, (c + 1) * rangesToAssign).sorted
      }
    }

    assert(otherRangeMap.values.forall(rs => rs == rs.sorted), "All range lists should be sorted")

    val combinedRanges = otherRangeMap.map(i => (i._1, combineRanges(i._2))).toMap

    for (d <- hypercube.get.dimensionSizes.indices) {
      val rangesForDimesion = otherRangeMap.filter(_._1._1 == d).values.flatten.toSeq
      assertNoneOverlapping(rangesForDimesion, d)
      assertCompleteness(rangesForDimesion, lower, upper, d)
    }

    // Translate range maps.
    for (p <- 0 until numberOfPartitions) {
      val coordinate = hypercube.get.getCoordinate(p)
      for (d <- hypercube.get.dimensionSizes.indices) {
        rangeMap((p, d)) = combinedRanges((d, coordinate(d)))
      }
    }
    rangesOpt = Option(rangeMap.toMap)
    rangesOpt.get
  }

  private def combineRanges(ranges: Seq[(Int, Int)]): Seq[(Int, Int)] = {
    val newRanges = mutable.Buffer[(Int, Int)]()

    var startRange = ranges.head
    var endRange = ranges.head

    for (r <- ranges.tail) {
      if (r._1 == endRange._2) {
        endRange = r
      } else {
        newRanges.append((startRange._1, endRange._2))
        startRange = r
        endRange = r
      }
    }

    newRanges.append((startRange._1, endRange._2))

    newRanges
  }

  private def assertNoneOverlapping(ranges: Seq[(Int, Int)], dimension: Int): Unit = {
    val allNumbers = ranges.flatMap(r => r._1 until r._2)

    assert(allNumbers.size == allNumbers.toSet.size, s"Dimension $dimension is overlapping ${ranges.mkString(", ")}")
  }


  @elidable(elidable.ASSERTION)
  private def assertCompleteness(ranges: Seq[(Int, Int)], lower: Int, upper: Int, dimension: Int): Unit = {
    val allNumbers = mutable.Set(lower until upper: _*)

    ranges.foreach(r => (r._1 until r._2).foreach(i => allNumbers.remove(i)))
    assert(allNumbers.isEmpty, s"Dimension $dimension was not complete, missing numbers: ${allNumbers.mkString(", ")}")
  }

  private def getPartitionBoundsInRange(lower: Int, upper: Int, partition: Int, numPartitions: Int, fromBelow: Boolean): (Int, Int) = {
    val totalSize = upper - lower
    val partitionSize = totalSize / numPartitions
    val lowerBound = if (fromBelow) {
      lower + partition * partitionSize
    } else {
      if (partition == numPartitions - 1) {
        lower
      } else {
        upper - (partition + 1) * partitionSize
      }
    }

    val upperBound = if (fromBelow) {
      if (partition == numPartitions - 1) {
        upper
      } else {
        lower + (partition + 1) * partitionSize
      }
    } else {
      upper - partition * partitionSize
    }
    (lowerBound, upperBound)
  }
}

case class SingleVariablePartitioning(variable: Int) extends Partitioning {

  def getEquivalentSharesRangePartitioning(parallelism: Int, numVariables: Int): SharesRange = {
    val dimensions = Array.fill(numVariables)(1)
    dimensions(variable) = parallelism
    SharesRange(Some(Hypercube(dimensions)))
  }

  override def getWorkersUsed(workersTotal: Int): Int = {
    workersTotal
  }
}

case class FirstVariablePartitioningWithWorkstealing(batchSize: Int = 1) extends Partitioning {
  var queueID = -1

  override def getWorkersUsed(workersTotal: Int): Int = {
    workersTotal
  }

  override def toString: String = {
    "FirstVariablePartitioningWithWorkstealing"
  }
}

object FirstVariablePartitioningWithWorkstealing {
  val logger = LoggerFactory.getLogger(FirstVariablePartitioningWithWorkstealing.getClass)

  val queues: mutable.Map[Int, ConcurrentLinkedQueue[Int]] = mutable.HashMap()

  def newQueue(id: Int, content: Seq[Int]): Int =  synchronized {
    if (!queues.isDefinedAt(id)) {
      val q = new ConcurrentLinkedQueue[Int]()
      q.addAll(content.asJava)
      queues.update(id, q)
    }
    id
  }

  def getQueue(id: Int): ConcurrentLinkedQueue[Int] = {
    queues(id)
  }
}

case class AllTuples() extends Partitioning {
  override def getWorkersUsed(workersTotal: Int): Int = {
    workersTotal
  }
}

object Partitioning {

  implicit def partitioningRead: scopt.Read[Partitioning] = {
    val singleVariablePartitioningPattern = raw"single\[(\d+)\]".r
    val sharesRangePartitioningPattern = raw"sharesRange\[(\d+)\]".r
    scopt.Read.reads({
      case "allTuples" => {
        AllTuples()
      }
      case "shares" => {
        Shares()
      }
      case "workstealing" => {
        FirstVariablePartitioningWithWorkstealing()
      }
      case sharesRangePartitioningPattern(prefix) => {
        val intPrefix = prefix.toInt
        SharesRange(None, if (intPrefix == 0) {
          None
        } else {
          Some(intPrefix)
        })
      }
      case singleVariablePartitioningPattern(v) => {
        SingleVariablePartitioning(v.toInt)
      }
      case _ => {
        throw new IllegalArgumentException("Partitionings can be only `allTuples` or `shares`")
      }
    })
  }
}