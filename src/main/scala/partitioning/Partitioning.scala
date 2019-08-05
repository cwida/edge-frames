package partitioning

import partitioning.shares.{Hypercube}

sealed trait Partitioning {

}

case class Shares(hypercube: Hypercube = Hypercube(Array[Int]())) extends Partitioning {
  override def toString: String = {
    if (hypercube.dimensionSizes.isEmpty) {
      "Shares(Uninitialized)"
    } else {
      super.toString
    }
  }
}

case class AllTuples() extends Partitioning

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