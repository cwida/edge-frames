package partitioning

import partitioning.shares.{Hypercube}

sealed trait Partitioning {
  def getWorkersUsed(workersTotal: Int): Int
}

case class Shares(hypercube: Hypercube = Hypercube(Array[Int]())) extends Partitioning {
  override def toString: String = {
    if (hypercube.dimensionSizes.isEmpty) {
      "Shares(Uninitialized)"
    } else {
      s"Shares(${hypercube.dimensionSizes.mkString(", ")})"
    }
  }

  override def getWorkersUsed(workersTotal: Int): Int = {
    hypercube.dimensionSizes.product
  }
}

case class SharesRange(hypercube: Option[Hypercube] = None, prefix: Option[Int] = None) extends Partitioning {

  override def toString: String = {
    hypercube match {
      case Some(Hypercube(dimensionSizes)) => {
        s"SharesRange(${dimensionSizes.mkString(", ")})" // TODO no commas in name because that ruins CSVs
      }
      case None => {
        s"SharesRange(Uninitialized, prefix=$prefix)"
      }
    }
  }

  override def getWorkersUsed(workersTotal: Int): Int = {
    hypercube.get.dimensionSizes.product
  }

  def getRanges(partition: Int, dimension: Int, lower: Int, upper: Int): Array[Int] = {
    val coordinate = hypercube.get.getCoordinate(partition)

    val (l, u) = getPartitionBoundsInRange(lower, upper, coordinate(dimension), hypercube.get.dimensionSizes(dimension), true)
    Array(l, u)
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

  // TODO should I print this as name?
  override def getWorkersUsed(workersTotal: Int): Int = {
    workersTotal
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
      case sharesRangePartitioningPattern(prefix) => {
        val intPrefix = prefix.toInt
        SharesRange(None, if (intPrefix == 0) None else Some(intPrefix))
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