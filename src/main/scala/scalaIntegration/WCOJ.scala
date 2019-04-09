package scalaIntegration

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.types.IntegerType

case class WCOJ(joinSpecification: JoinSpecification, child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] = {
    joinSpecification.allVariables.map(name => {
      val reference = if (joinSpecification.bindsOnFirstLevel(name)) child.output(0) else child.output(1)
      AttributeReference(name, IntegerType, false)(reference.exprId)
    })
  }
}
