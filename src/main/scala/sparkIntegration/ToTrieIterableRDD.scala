package sparkIntegration

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}

// For testing
case class ToTrieIterableRDD(child: LogicalPlan, attributeOrdering: Seq[String]) extends UnaryNode {
  override def output: Seq[Attribute] = if (attributeOrdering == Seq("dst", "src")) { child.output.reverse } else { child.output }
}
