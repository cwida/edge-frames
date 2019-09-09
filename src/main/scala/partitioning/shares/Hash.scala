package partitioning.shares

import scala.util.hashing.MurmurHash3

class Hash(seed: Int, max: Int) {

  /**
    * Hashes i to a number between 0 and max (max exclusive)
    * @param i
    * @return
    */
  def hash(i: Int): Int = {
    Math.abs(MurmurHash3.finalizeHash(MurmurHash3.mix(seed, i), 1)) % max
  }

}
