package scalaIntegration

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.types.IntegerType

case class WCOJ(joinSpecification: JoinSpecification, child: LogicalPlan) extends UnaryNode {

  override def references: AttributeSet = AttributeSet(child.output.filter(a => List("src", "dst").contains(a.name)))

  override def output: Seq[Attribute] = {
    joinSpecification.allVariables.map(name => {
      val reference = if (joinSpecification.bindsOnFirstLevel(name))
        child.output.filter(a => a.name == "src").head
      else
        child.output.filter(a => a.name == "dst").head
      AttributeReference(name, IntegerType, false)(reference.exprId)
    })
  }
}
