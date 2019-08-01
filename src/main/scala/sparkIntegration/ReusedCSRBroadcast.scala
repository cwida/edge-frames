package org.apache.spark.sql

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode
import sparkIntegration.CSRCache

case class ReusedCSRBroadcast(rddId: Long) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException("Does not support the doExecutre code path.")
  }

  override protected[sql] def doExecuteBroadcast[T](): Broadcast[T] = {
    CSRCache.get(rddId).head.asInstanceOf[Broadcast[T]]
  }

  override def output: Seq[Attribute] = Seq()  // TODO
}
