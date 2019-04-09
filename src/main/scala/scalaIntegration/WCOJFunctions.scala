package org.apache.spark.sql

import org.apache.spark.sql.types.IntegerType
import scalaIntegration.{JoinSpecification, Pattern, WCOJ}

import Predef._

class WCOJFunctions[T](ds: Dataset[T]) {
  def findPattern(pattern: String, variableOrdering: Seq[String]) = {
    require(ds.columns.contains("src"), "Edge table should have a column called `src`")
    require(ds.columns.contains("dst"), "Edge table should have a column called `dst`")

    require(ds.col("src").expr.dataType == IntegerType, "Edge table src needs to be an integer")
    require(ds.col("dst").expr.dataType == IntegerType, "Edge table src needs to be an integer")

    Dataset.ofRows(ds.sparkSession, new WCOJ(new JoinSpecification(Pattern.parse(pattern), variableOrdering), ds.logicalPlan))
  }
}
