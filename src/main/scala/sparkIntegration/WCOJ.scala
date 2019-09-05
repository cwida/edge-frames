package sparkIntegration

import leapfrogTriejoin.TreeTrieIterator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.slf4j.LoggerFactory

import Predef._

case class WCOJ(graphID: Int, ouputVariables: Seq[Attribute], joinSpecification: JoinSpecification, children: Seq[LogicalPlan],
                partitionChild: LogicalPlan, graphCSRFile: String = "") extends LogicalPlan {

  override def references: AttributeSet = {
    AttributeSet(children.flatMap(c => c.output.filter(a => List("src", "dst").contains(a.name))))
  }

  override def output: Seq[Attribute] = {
    ouputVariables
  }
}
