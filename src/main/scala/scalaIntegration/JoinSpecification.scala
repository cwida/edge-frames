package scalaIntegration

import leapfrogTriejoin.{EdgeRelationship, LeapfrogTriejoin, TrieIterator}
import org.slf4j.LoggerFactory

class JoinSpecification(joinPattern: Seq[Pattern], variableOrdering: Seq[String]) extends Serializable {
  private val logger = LoggerFactory.getLogger(classOf[JoinSpecification])

  val allVariables: Seq[String] = variableOrdering

  def bindsOnFirstLevel(variable: String): Boolean = {
    joinPattern.exists { case AnonymousEdge(src: NamedVertex, dst: NamedVertex) => {
      src.name == variable
    }}
  }

  def build(tuples: Array[(Int, Int)]): LeapfrogTriejoin = {
    val trieIterators = joinPattern.map(p => p match {
      case AnonymousEdge(src: NamedVertex, dst: NamedVertex) => {
        new TrieIterator(new EdgeRelationship((src.name, dst.name), tuples))
      }
      case _ => throw new InvalidParseException("Use only anonymous edges with named vertices.")
      // TODO negated edges?
    })
    // TODO general variable order
    new LeapfrogTriejoin(trieIterators, variableOrdering)
  }
}