package sparkIntegration

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

object WCOJ2WCOJExec extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case WCOJ(outputVariables, joinSpecification, cs) => {
      WCOJExec(outputVariables, joinSpecification,
        joinSpecification.buildTrieIterables(cs.map(planLater))) :: Nil
    }
    case _ => Nil
  }
}

object ToTrieIterableRDD2ToTrieIterableRDDExec extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ToTrieIterableRDD(child, variableOrdering) => {
      ToArrayTrieIterableRDDExec(planLater(child), variableOrdering) :: Nil
    }
    case _ => Nil
  }
}
