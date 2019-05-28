package sparkIntegration

import leapfrogTriejoin.{TrieIterable, TrieIterator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.LongType
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}

class TwoTrieIterableRDD[S <: TrieIterable](val trieIterableRDD: RDD[(S, S)])
  extends RDD[InternalRow](trieIterableRDD.context, List(new OneToOneDependency(trieIterableRDD))) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    trieIterableRDD.compute(split, context).flatMap { case (lCSR, rCRS) => {
      lCSR.iterator.zip(rCRS.iterator).map { case (lt, rt) => {
        InternalRow(lt.toSeq(Seq(LongType, LongType)) ++ rt.toSeq(Seq(LongType, LongType)))
      }}
    }
    }
  }

  override protected def getPartitions: Array[Partition] = {
    trieIterableRDD.partitions
  }

  def trieIterables: RDD[(S, S)] = {
    trieIterableRDD
  }
}