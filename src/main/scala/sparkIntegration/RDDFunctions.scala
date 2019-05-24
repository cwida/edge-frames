package org.apache.spark.rdd

import org.apache.spark.SparkContext


import scala.reflect.ClassTag


class RDDFunctions[T: ClassTag](rdd: RDD[T]) {

  def generalZippedPartitions[V: ClassTag](spark: SparkContext, others: List[RDD[T]], preservesPartitioning: Boolean = false)(f: List[Iterator[T]] => Iterator[V]): RDD[V] ={
    new GeneralZippedPartitionsRDD(spark, spark.clean(f), rdd :: others, preservesPartitioning)
  }

}
