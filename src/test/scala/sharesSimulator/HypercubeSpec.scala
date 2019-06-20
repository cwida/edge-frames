package sharesSimulator

import experiments.{Clique}
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class HypercubeSpec extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {

  val alphabet = ('a' to 'z').map(c => s"$c")

  "For any configuration, it" should "map to all workers" in {
    val configurationGen = Gen.listOfN(5, Gen.chooseNum(1, 10))

    forAll (configurationGen) {
      conf => {
        whenever(conf.product != 0 && conf.size > 1) {
          val workers = conf.product
          val hypercube = new Hypercube(workers,
            Clique(conf.size), Some(conf))
          val allCoordinates = Utils.cartesian(conf.map(n => (0 until n).toList))

          val mappedCoordinates: Seq[Int] = allCoordinates.map(hypercube.coordinate2Worker)

          mappedCoordinates should contain theSameElementsAs (0 until workers).toList
        }
      }
    }
  }

}
