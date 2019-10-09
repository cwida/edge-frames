package sparkIntegration

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

case class WCOJ(graphID: Int, ouputVariables: Seq[Attribute], joinSpecification: JoinSpecification, children: Seq[LogicalPlan],
                partitionChild: LogicalPlan, graphCSRFile: String = "") extends LogicalPlan {

  override def references: AttributeSet = {
    AttributeSet(children.flatMap(c => c.output.filter(a => List("src", "dst").contains(a.name))))
  }

  override def output: Seq[Attribute] = {
    ouputVariables
  }
}
