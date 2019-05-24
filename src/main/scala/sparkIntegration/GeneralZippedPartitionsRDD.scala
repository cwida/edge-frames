package org.apache.spark.rdd

import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class GeneralZippedPartitionsRDD[A: ClassTag, V: ClassTag](sc: SparkContext,
                                                           var f: List[Iterator[A]] => Iterator[V],
                                                           rdds: Seq[RDD[A]],
                                                           preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, rdds, preservesPartitioning) {

  override def compute(split: Partition, context: TaskContext): Iterator[V] = {
    val partitions = split.asInstanceOf[ZippedPartitionsPartition].partitions
    f(rdds.zipWithIndex.map( { case (rdd, i) => rdd.iterator(partitions(i), context) }).toList)
  }
}
