package sparkIntegration

import leapfrogTriejoin.{EdgeRelationship, LeapfrogTriejoin, TreeTrieIterator, TrieIterable}
import org.slf4j.LoggerFactory

import scala.collection.mutable

class JoinSpecification(joinPattern: Seq[Pattern], variableOrdering: Seq[String]) extends Serializable {
  private val logger = LoggerFactory.getLogger(classOf[JoinSpecification])

  val allVariables: Seq[String] = variableOrdering
  val edges: Seq[Pattern] = joinPattern

  private val variable2RelationshipIndex = mutable.Map[String, (Int, Int)]()
  for ((p, i) <- joinPattern.zipWithIndex) {
    p match {
      case AnonymousEdge(src: NamedVertex, dst: NamedVertex) => {
        if (!variable2RelationshipIndex.contains(src.name)) {
          variable2RelationshipIndex.update(src.name, (i.toInt, 0))
        }
        if (!variable2RelationshipIndex.contains(dst.name)) {
          variable2RelationshipIndex.update(dst.name, (i.toInt, 1))
        }
      }
    }
  }

  def variableToRelationshipIndex(variable: String): Int = {
    variable2RelationshipIndex(variable)._1
  }

  def variableToAttributeIndex(variable: String): Int = {
    variable2RelationshipIndex(variable)._2
  }

  def bindsOnFirstLevel(variable: String): Boolean = {
    joinPattern.exists { case AnonymousEdge(src: NamedVertex, dst: NamedVertex) => {
      src.name == variable
    }}
  }

  def build(trieIterable: TrieIterable): LeapfrogTriejoin = {
    val trieIterators = joinPattern.map {
      case AnonymousEdge(src: NamedVertex, dst: NamedVertex) => {
        (new EdgeRelationship((src.name, dst.name)), trieIterable.trieIterator)
      }
      case _ => throw new InvalidParseException("Use only anonymous edges with named vertices.")
      // TODO negated edges?
    }.toMap
    // TODO general variable order
    new LeapfrogTriejoin(trieIterators, variableOrdering)
  }

}