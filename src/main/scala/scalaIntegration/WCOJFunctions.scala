package scalaIntegration

import org.apache.spark.sql.{DataFrame, Dataset}

class WCOJFunctions[T](ds: Dataset[T]) {
  def cachedGraphTopology(): DataFrame = {
    Dataset.ofRows(ds.sparkSession, WCOJ(ds.logicalPlan))
  }
}
