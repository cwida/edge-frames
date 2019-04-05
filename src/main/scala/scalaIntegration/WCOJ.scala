package scalaIntegration

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}

case class WCOJ(joinSpecification: JoinSpecification, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}
