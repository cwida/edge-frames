package sparkIntegration

import experiments.{Algorithm, GraphWCOJ, WCOJAlgorithm}
import leapfrogTriejoin.{CSRTrieIterable, EdgeRelationship, LeapfrogTriejoin, TrieIterable}
import org.apache.spark.sql.CSRTrieIterableBroadcast
import org.apache.spark.sql.execution.SparkPlan
import org.slf4j.LoggerFactory
import partitioning.{AllTuples, Partitioning, RangeFilteredTrieIterator, RoundRobin, Shares, SharesRange}
import sparkIntegration.wcoj.ToArrayTrieIterableRDDExec

import scala.collection.mutable

class JoinSpecification(joinPattern: Seq[Pattern], val variableOrdering: Seq[String],
                        val joinAlgorithm: WCOJAlgorithm,
                        partitioning: Partitioning,
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
      case _ => {
        throw new IllegalArgumentException("Illegal join pattern used.")
      }
    }
  }

  private val dstAccessibleRelationships = joinPattern.zipWithIndex
    .filter({ case (AnonymousEdge(src: NamedVertex, dst: NamedVertex), _) => {
      variableOrdering.indexOf(dst.name) < variableOrdering.indexOf(src.name)

    }
    case _ => {
      throw new IllegalArgumentException("Illegal join pattern used.")
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
    case _ => {
      throw new IllegalArgumentException("Illegal join pattern used.")
    }
    }
  }

  def build(trieIterables: Seq[TrieIterable], partition: Int, partitions: Int): LeapfrogTriejoin = {
    val trieIterators = joinAlgorithm match {
      case experiments.WCOJ => {
        trieIterables.map(_.trieIterator)
      }
      case GraphWCOJ => {
        joinPattern.zipWithIndex.map({
          case (AnonymousEdge(src: NamedVertex, dst: NamedVertex), i) => {
            partitioning match {
              case part@SharesRange(_, _) => {
                if (dstAccessibleRelationship(i)) {
                  val trieIterable = trieIterables(1).asInstanceOf[CSRTrieIterable]

                  val firstDimension = variableOrdering.indexOf(dst.name)
                  val secondDimension = variableOrdering.indexOf(src.name)

                  val ti = trieIterable.trieIterator
                  val firstDimensionRanges = part.getRanges(
                    partition, firstDimension, trieIterable.minValue, trieIterable.maxValue)
                    .flatMap(r => Seq(r._1, r._2)).toArray
                  val secondDimensionRanges = part.getRanges(
                    partition, secondDimension, trieIterable.minValue, trieIterable.maxValue)
                    .flatMap(r => Seq(r._1, r._2)).toArray

                  new RangeFilteredTrieIterator(partition, firstDimensionRanges, secondDimensionRanges, ti)
                  //                  trieIterables(1).asInstanceOf[CSRTrieIterable].trieIterator(
                  //                    partition,
                  //                    partitioning,
                  //                    variableOrdering.indexOf(dst.name),
                  //                    variableOrdering.indexOf(src.name)
                  //                  )
                } else {
                  val trieIterable = trieIterables.head.asInstanceOf[CSRTrieIterable]
                  val firstDimension = variableOrdering.indexOf(src.name)
                  val secondDimension = variableOrdering.indexOf(dst.name)
                  val ti = trieIterable.trieIterator(partition, partitions, partitioning, firstDimension, secondDimension)
                  val firstDimensionRanges = part.getRanges(
                    partition, firstDimension, trieIterable.minValue, trieIterable.maxValue)
                    .flatMap(r => Seq(r._1, r._2)).toArray
                  val secondDimensionRanges = part.getRanges(
                    partition, secondDimension, trieIterable.minValue, trieIterable.maxValue)
                    .flatMap(r => Seq(r._1, r._2)).toArray

                  new RangeFilteredTrieIterator(partition, firstDimensionRanges, secondDimensionRanges, ti)
                  // TODO use TrieIterator directly for a single range
                  //                  new MultiRangePartitionTrieIterator(Array(trieIterable.minValue, trieIterable.maxValue), Array(trieIterable.minValue, trieIterable.maxValue), ti)
                  //                  trieIterables(0).asInstanceOf[CSRTrieIterable].trieIterator(
                  //                    partition,
                  //                    partitioning,
                  //                    variableOrdering.indexOf(src.name),
                  //                    variableOrdering.indexOf(dst.name)
                  //                  )
                }
              }
              case RoundRobin(v) => {
                val trieIterable = trieIterables.head.asInstanceOf[CSRTrieIterable]

                val firstDimension = variableOrdering.indexOf(src.name)
                val secondDimension = variableOrdering.indexOf(dst.name)

                trieIterable.trieIterator(partition, partitions, partitioning, firstDimension, secondDimension)
              }
              case _ => {
                if (dstAccessibleRelationship(i)) {
                  trieIterables(1).trieIterator
                } else {
                  trieIterables(0).trieIterator
                }
              }
            }
          }
          case (_, _) => {
            throw new IllegalArgumentException("Illegal join pattern.")
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

    new LeapfrogTriejoin(trieIteratorMapping, variableOrdering, distinctFilter, smallerThanFilter, partition, partitioning)
  }

  def buildTrieIterables(children: Seq[SparkPlan], graphID: Int): Seq[SparkPlan] = {
    joinAlgorithm match {
      case experiments.WCOJ => {
        children.zipWithIndex.map({ case (c, i) => {
          val attributeOrdering = if (!dstAccessibleRelationship(i)) {
            Seq("src", "dst")
          } else {
            Seq("dst", "src")
          }
          ToArrayTrieIterableRDDExec(c, attributeOrdering)
        }
        })
      }
      case GraphWCOJ => {
        require(children.size == 2, "GraphWCOJ requires exactly two children.")
        Seq(CSRTrieIterableBroadcast(graphID, children.head, children(1)))
      }
    }
  }

}