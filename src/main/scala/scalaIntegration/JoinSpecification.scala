package scalaIntegration

import leapfrogTriejoin.LeapfrogTriejoin
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow

abstract class JoinSpecification {
  def build(rows : RDD[InternalRow]): LeapfrogTriejoin
}
