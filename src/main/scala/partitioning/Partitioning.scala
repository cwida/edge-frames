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
          Shares(Hypercube(Array[Int]()))
        }
        case _ => {
          throw new IllegalArgumentException("Partitionings can be only `allTuples` or `shares`")
        }
      })
    }
}