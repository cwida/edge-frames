package partitioning

import partitioning.shares.{Hypercube}

sealed trait Partitioning {
  def getWorkersUsed(workersTotal:Int) : Int
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

case class SharesRange(hypercube: Hypercube = Hypercube(Array[Int]())) extends Partitioning {
  override def toString: String = {
    if (hypercube.dimensionSizes.isEmpty) {
      "SharesRange(Uninitialized)"
    } else {
      s"SharesRange(${hypercube.dimensionSizes.mkString(", ")})"
    }
  }

  override def getWorkersUsed(workersTotal: Int): Int = {
    hypercube.dimensionSizes.product
  }
}

case class SingleVariablePartitioning(variable: Int) extends Partitioning {

  def getEquivalentSharesRangePartitioning(parallelism: Int, numVariables: Int): SharesRange = {
    val dimensions = Array.fill(numVariables)(1)
    dimensions(variable) = parallelism
    SharesRange(Hypercube(dimensions))
  }

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
      scopt.Read.reads({
        case "allTuples" => {
          AllTuples()
        }
        case "shares" => {
          Shares()
        }
        case "sharesRange" => {
          SharesRange()
        }
        case _ => {
          throw new IllegalArgumentException("Partitionings can be only `allTuples` or `shares`")
        }
      })
    }
}