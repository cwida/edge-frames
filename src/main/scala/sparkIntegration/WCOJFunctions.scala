package org.apache.spark.sql

import org.apache.spark.sql.types.IntegerType
import sparkIntegration.{JoinSpecification, Pattern, WCOJ}

import Predef._

class WCOJFunctions[T](ds: Dataset[T]) {
  def findPattern(pattern: String, variableOrdering: Seq[String]) = {
    require(ds.columns.contains("src"), "Edge table should have a column called `src`")
    require(ds.columns.contains("dst"), "Edge table should have a column called `dst`")

    require(ds.col("src").expr.dataType == IntegerType, "Edge table src needs to be an integer")
    require(ds.col("dst").expr.dataType == IntegerType, "Edge table src needs to be an integer")

    val edges = Pattern.parse(pattern)

    val joinSpecification = new JoinSpecification(edges, variableOrdering)
    val children = edges.zipWithIndex.map { case (p, i) => ds.alias(s"edges_${i.toString}")
      .withColumnRenamed("src", s"src")  // Needed to guarantee that src and dst on the aliases are referenced by different attributes.
      .withColumnRenamed("dst", s"dst")
      .logicalPlan}

    Dataset.ofRows(ds.sparkSession, new WCOJ(joinSpecification, children))
  }
}
