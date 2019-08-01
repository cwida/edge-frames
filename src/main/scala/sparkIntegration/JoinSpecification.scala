package sparkIntegration

import experiments.{Algorithm, GraphWCOJ}
import leapfrogTriejoin.{EdgeRelationship, LeapfrogTriejoin, TrieIterable}
import org.apache.spark.sql.ToCSRTrieIterableRDDExec
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
    val trieIterators = joinAlgorithm match {
      case experiments.WCOJ => {
        trieIterables.map(_.trieIterator)
      }
      case GraphWCOJ => {
        joinPattern.zipWithIndex.map({
          case (_, i) => {
            if (dstAccessibleRelationship(i)) {
              trieIterables(1).trieIterator
            } else {
              trieIterables(0).trieIterator
            }
          }
        })
      }
    }
    val trieIteratorMapping = joinPattern.zipWithIndex.map({
      case (AnonymousEdge(src: NamedVertex, dst: NamedVertex), i) => {
        if (dstAccessibleRelationship(i)) {
          (new EdgeRelationship((dst.name, src.name)), trieIterators(i))

        } else {
          (new EdgeRelationship((src.name, dst.name)), trieIterators(i))
        }
      }
      case _ => {
        throw new InvalidParseException("Use only anonymous edges with named vertices.")
      }
    }).toMap
    new LeapfrogTriejoin(trieIteratorMapping, variableOrdering, distinctFilter, smallerThanFilter)
  }

  def buildTrieIterables(children: Seq[SparkPlan], graphID: Int): Seq[SparkPlan] = {
    joinAlgorithm match {
      case experiments.WCOJ => {
        children.zipWithIndex.map({ case (c, i) =>
        val attributeOrdering = if (!dstAccessibleRelationship(i)) {
          Seq("src", "dst")
        } else {
          Seq("dst", "src")
        }

        ToArrayTrieIterableRDDExec(c, attributeOrdering)
      })}
      case GraphWCOJ => {
        Seq(ToCSRTrieIterableRDDExec(children, graphID))
      }
    }
  }

}