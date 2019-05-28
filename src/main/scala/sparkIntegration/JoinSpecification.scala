package sparkIntegration

import experiments.{Algorithm, GraphWCOJ}
import leapfrogTriejoin.{EdgeRelationship, LeapfrogTriejoin, TrieIterable}
import org.apache.spark.sql.execution.SparkPlan
import org.slf4j.LoggerFactory

import scala.collection.mutable

class JoinSpecification(joinPattern: Seq[Pattern], val variableOrdering: Seq[String],
                        joinAlgorithm: Algorithm,
                        val distinctFilter: Boolean,
                        smallerThanFilter: Boolean) extends Serializable {
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

  private val dstAccessibleRelationships = joinPattern.zipWithIndex
    .filter({ case (AnonymousEdge(src: NamedVertex, dst: NamedVertex), _) => {
      variableOrdering.indexOf(dst.name) < variableOrdering.indexOf(src.name)
    }
    })
    .map(_._2)

  def dstAccessibleRelationship(rel: Int): Boolean = {
    dstAccessibleRelationships.contains(rel)
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
    }
    }
  }

  def build(trieIterables: Seq[TrieIterable]): LeapfrogTriejoin = {
    val trieIterators = joinPattern.zipWithIndex.map({
      case (AnonymousEdge(src: NamedVertex, dst: NamedVertex), i) => {
        if (dstAccessibleRelationship(i)) {
          (new EdgeRelationship((dst.name, src.name)), trieIterables(i).trieIterator)

        } else {
          (new EdgeRelationship((src.name, dst.name)), trieIterables(i).trieIterator)
        }
      }
      case _ => {
        throw new InvalidParseException("Use only anonymous edges with named vertices.")
      }
      // TODO negated edges?
    }).toMap
    new LeapfrogTriejoin(trieIterators, variableOrdering, distinctFilter, smallerThanFilter)
  }

  def buildTrieIterable(childPlan: SparkPlan, childIndex: Int): SparkPlan = {
    val attributeOrdering = if (!dstAccessibleRelationship(childIndex)) {
      Seq("src", "dst")
    } else {
      Seq("dst", "src")
    }
    joinAlgorithm match {
      case experiments.WCOJ => {
        ToArrayTrieIterableRDDExec(childPlan, attributeOrdering)
      }
      case GraphWCOJ => {
        ToArrayTrieIterableRDDExec(childPlan, attributeOrdering) // TODO fix
      }
    }

  }

}