package sharesSimulator

import scala.util.hashing.MurmurHash3

// TODO much shorter hash functions for ints: https://burtleburtle.net/bob/hash/integer.html
case class Hash(seed: Int, max: Int) {

  /**
    * Hashes i to a number between 0 and max (max exclusive)
    * @param i
    * @return
    */
  def hash(i: Int): Int = {
    Math.abs(MurmurHash3.finalizeHash(MurmurHash3.mix(seed, i), 1)) % max
  }

  def test: Unit = {
    require(hash(42) == hash(42), "Returns the same hash per number")

    val hashes = (0 to 100000).map(hash).toSeq
    require(hashes.forall(_ > 0))
    require(hashes.forall(_ < max))
    require(between(hashes.sum.toDouble / hashes.size, max / 2 + 0.5, max / 2 - 0.5))
  }

  private def between(v: Double, upper: Double, lower: Double): Boolean = {
    v <= upper && lower <= v
  }
}
