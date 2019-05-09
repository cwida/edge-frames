package org.apache.spark.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.IntegerType
import sparkIntegration.{JoinSpecification, Pattern, ToTrieIterableRDD, WCOJ}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.RowIterator

import Predef._

class WCOJFunctions[T](ds: Dataset[T]) {
  def findPattern(pattern: String, variableOrdering: Seq[String], distinctFilter: Boolean = false): DataFrame = {
    val edges = Pattern.parse(pattern)

    val children = edges.zipWithIndex.map { case (_, i) => {
      ds.alias(s"edges_${i.toString}")
        // TODO can I remove this now?
        .withColumnRenamed("src", s"src") // Needed to guarantee that src and dst on the aliases are referenced by different attributes.
        .withColumnRenamed("dst", s"dst")
    }
    }
    findPattern(pattern, variableOrdering, distinctFilter, children)
  }

  def findPattern(pattern: String, variableOrdering: Seq[String], distinctFilter: Boolean, children: Seq[DataFrame]): DataFrame = {

    require(ds.columns.contains("src"), "Edge table should have a column called `src`")
    require(ds.columns.contains("dst"), "Edge table should have a column called `dst`")

    require(ds.col("src").expr.dataType == IntegerType, "Edge table src needs to be an integer")
    require(ds.col("dst").expr.dataType == IntegerType, "Edge table src needs to be an integer")

    val edges = Pattern.parse(pattern)

    require(edges.size == children.size, "WCOJ needs as many children as edges in the pattern.")

    val joinSpecification = new JoinSpecification(edges, variableOrdering, distinctFilter)

    val outputVariables = joinSpecification.variableOrdering.map(v => AttributeReference(v, IntegerType, nullable = false)())

    Dataset.ofRows(ds.sparkSession, WCOJ(outputVariables, joinSpecification, children.map(_.logicalPlan)))
  }

  /**
    * Currently, not used!
    *
    * Creates an edge relationship from a dataset.
    *
    * The input dataset is required to have two integer attributes called `src` and `dst`.
    * The returned dataset will be:
    *   1. projected on these two attributes
    *   2. if `allowArbritaryVariableOrderings` then each edge will exist in both directions
    *   3. tagged with a boolean value which is true for edges from the original dataset
    *   4. sorted by `src`, `dst` ASC.
    *
    * @param allowArbritaryVariableOrderings allows to query the dataset with arbitrary variable orderings in a WCOJ.
    * @param isUndirected                    if `true` the dataset is assumed to be of an undirected graph with edges that exist only in one direction,
    *                                        that saves some time during construction
    * @param isUndirectedDuplicated          if `true` the dataset is assumed to be of an undirected graph with
    *                                        edges in both direction already existing, which saves even more time during construction.
    * @return
    */
  def toEdgeRelationship(allowArbritaryVariableOrderings: Boolean = true,
                         isUndirected: Boolean = false,
                         isUndirectedDuplicated: Boolean = false): Dataset[(Int, Int, Boolean)] = {
    import ds.sparkSession.implicits._

    require(ds.columns.contains("src"), "Edge table should have a column called `src`")
    require(ds.columns.contains("dst"), "Edge table should have a column called `dst`")

    require(ds.col("src").expr.dataType == IntegerType, "Edge table src needs to be an integer")
    require(ds.col("dst").expr.dataType == IntegerType, "Edge table src needs to be an integer")

    val projected = ds.select("src", "dst").as[(Int, Int)]
    val duplicated: Dataset[(Int, Int, Boolean)] =
      if (allowArbritaryVariableOrderings && !isUndirectedDuplicated) {
        projected.flatMap { case (src, dst) => {
          Seq((src, dst, true), (dst, src, false))
        }
        }
      } else {
        projected.map { case (src, dst) => {
          (src, dst, true)
        }
        }
      }

    val sorted = duplicated.sort("_1", "_2")

    if (allowArbritaryVariableOrderings && !isUndirected && !isUndirectedDuplicated) {
      // Remove duplicates that existed already
      sorted.mapPartitions(iter => {
        new Iterator[(Int, Int, Boolean)] {
          var lookBack: (Int, Int, Boolean) = if (iter.hasNext) {
            iter.next()
          } else {
            null
          }

          override def hasNext: Boolean = {
            iter.hasNext || lookBack != null
          }

          override def next(): (Int, Int, Boolean) = {
            if (iter.hasNext) {
              val n = iter.next()
              if (n._1 == lookBack._1 && n._2 == lookBack._2) {
                lookBack = null
                (n._1, n._2, true)
              } else {
                val ret = lookBack
                lookBack = n
                ret
              }
            } else {
              val temp = lookBack // Cannot be null because `hasNext` returned `true`
              lookBack = null
              temp
            }
          }
        }
      })
    } else {
      sorted
    }.withColumnRenamed("_1", "src").withColumnRenamed("_1", "dst").as[(Int, Int, Boolean)]
  }

  // For testing
  def toTrieIterableRDD(variableOrdering: Seq[String]): DataFrame = {
    Dataset.ofRows(ds.sparkSession, ToTrieIterableRDD(ds.logicalPlan, variableOrdering))
  }
}
