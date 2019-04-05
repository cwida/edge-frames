package org.apache.spark.sql

import scalaIntegration.{JoinSpecification, Pattern, WCOJ}
class WCOJFunctions[T](ds: Dataset[T]) {
  def findPattern(pattern: String, variableOrdering: Seq[String]) = {
    Dataset.ofRows(ds.sparkSession, new WCOJ(new JoinSpecification(Pattern.parse(pattern), variableOrdering), ds.logicalPlan))
  }
}
