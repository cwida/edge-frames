package partitioning

import partitioning.shares.{Hypercube}

sealed trait Partitioning {

}

case class Shares(hypercube: Hypercube) extends Partitioning {}

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