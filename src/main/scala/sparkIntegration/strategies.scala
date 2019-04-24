package sparkIntegration

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

object WCOJ2WCOJExec extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case WCOJ(outputVariables, joinSpecification, cs) => {
      WCOJExec(outputVariables, joinSpecification,  cs.zipWithIndex.map( { case (c, i) =>
        ToTrieIterableRDDExec(planLater(c),
          if ( !joinSpecification.dstAccessibleRelationship(i)) Seq("src", "dst") else Seq("dst", "src")) })) :: Nil
    }
    case _ => Nil
  }
}

object ToTrieIterableRDD2ToTrieIterableRDDExec extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ToTrieIterableRDD(child, variableOrdering) => {
      ToTrieIterableRDDExec(planLater(child), variableOrdering) :: Nil
    }
    case _ => Nil
  }
}
