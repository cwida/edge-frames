package leapfrogTriejoin

import org.scalatest.FlatSpec

class UnaryRelationshipSpec extends FlatSpec {

  "seek " should "find existing keys" in {
    val rel = new UnaryRelationship(Array(1, 2))
    rel.seek(2)
    assert(rel.key == 2)
  }

  "seek " should "move to the next bigger value if value does not exist " in {
    val rel = new UnaryRelationship(Array(1, 3))
    rel.seek(2)
    assert(rel.key == 3)
  }

}
