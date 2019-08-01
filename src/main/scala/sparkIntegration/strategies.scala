package sparkIntegration

import org.apache.spark.sql.{ReusedCSRBroadcast, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

object WCOJ2WCOJExec extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case WCOJ(graphID, outputVariables, joinSpecification, cs, partitionChild) => {
      val children = CSRCache.get(graphID) match {
         case None =>
           joinSpecification.buildTrieIterables(cs.map(planLater), graphID)
         case Some(b) =>
           Seq(ReusedCSRBroadcast(graphID))
       }
      DistributedWCOJExec(outputVariables, joinSpecification, children.head, planLater(partitionChild)) :: Nil
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
