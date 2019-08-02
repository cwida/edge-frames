package partitioning.shares

import scala.util.hashing.MurmurHash3

// TODO much shorter hash functions for ints: https://burtleburtle.net/bob/hash/integer.html
class Hash(seed: Int, max: Int) {

  private[this] val values = Array.fill(1500000)(-1)

  /**
    * Hashes i to a number between 0 and max (max exclusive)
    * @param i
    * @return
    */
  def hash(i: Int): Int = {
//    if (values(i) == -1) {
      val hash = Math.abs(MurmurHash3.finalizeHash(MurmurHash3.mix(seed, i), 1)) % max
//      values(i) = hash
//    }
//    values(i)
    hash
  }

}
