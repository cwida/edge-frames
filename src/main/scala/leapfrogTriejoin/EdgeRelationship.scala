package leapfrogTriejoin

class EdgeRelationship(private val _variables: (String, String)) {
  val variables = List(_variables._1, _variables._2)
}
