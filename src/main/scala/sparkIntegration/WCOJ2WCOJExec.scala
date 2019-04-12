package sparkIntegration

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

object WCOJ2WCOJExec extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case WCOJ(joinSpecification, c :: cs) => {
      WCOJExec(joinSpecification, new ToTrieIterableRDDExec(planLater(c)) :: cs.map(planLater)) :: Nil
    }
    case _ => Nil
  }
}
