package sparkIntegration

import experiments.GraphWCOJ
import org.apache.spark.sql.{ReusedCSRBroadcast, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import sparkIntegration.graphWCOJ.{CSRCache, GraphWCOJExec}
import sparkIntegration.wcoj.{ToArrayTrieIterableRDDExec, ToTrieIterableRDD, WCOJExec}

object WCOJ2WCOJExec extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case WCOJ(graphID, outputVariables, joinSpecification, cs, partitionChild, graphCSRFile) => {
      joinSpecification.joinAlgorithm match {
        case experiments.WCOJ => {
          val children = joinSpecification.buildTrieIterables(cs.map(planLater), graphID)
          WCOJExec(outputVariables, joinSpecification, children) :: Nil
        }
        case GraphWCOJ => {
          val graphChild = CSRCache.get(graphID) match {
            case None =>
              joinSpecification.buildTrieIterables(cs.map(planLater), graphID, graphCSRFile).head
            case Some(b) =>
              ReusedCSRBroadcast(graphID)
          }
          GraphWCOJExec(outputVariables, joinSpecification, graphChild, planLater(partitionChild)) :: Nil
        }
      }
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
