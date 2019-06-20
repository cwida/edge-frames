package sharesSimulator

import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class HashSpec extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {

  "Hash" should "return the same value for the same input" in {
    val inputGen = Gen.posNum[Int]

    val hash = Hash(42, 100)

    forAll(inputGen) {
      input => {
        hash.hash(input) should equal(hash.hash(input))
      }
    }
  }

  "Hash" should "be positive" in {
    val inputGen = Gen.posNum[Int]

    val hash = Hash(42, 100)

    forAll(inputGen) {
      input => {
        hash.hash(input) should be >= 0
      }
    }
  }

  "Hash" should "be smaller than max" in {
    val inputGen = Gen.posNum[Int]

    val max = 100
    val hash = Hash(42, max)

    forAll(inputGen) {
      input => {
        hash.hash(input) should be < max
      }
    }
  }

  "Hash" should "be uniform over suffieciently large input data" in {
    val inputGen = Gen.listOfN(20000, Gen.posNum[Int])

    val max = 100
    val hash = Hash(42, max)

    forAll(inputGen) {
      input => {
        whenever(input.toSet.size > 200) {
          val hashes = input.toSet.map(hash.hash)
          (hashes.sum.toDouble / hashes.size) should equal (50.0 +- 1.5)
        }
      }
    }
  }

}
