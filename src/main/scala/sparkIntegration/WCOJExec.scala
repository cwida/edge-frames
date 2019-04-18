package sparkIntegration

import leapfrogTriejoin.{ArrayTrieIterable, TrieIterable}
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.{RowIterator, SparkPlan}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}

case class WCOJExec(joinSpecification: JoinSpecification, children: Seq[SparkPlan]) extends SparkPlan {

  override def output: Seq[Attribute] = {
    joinSpecification.allVariables.map(v => {
      val relI = joinSpecification.variableToRelationshipIndex(v)
      val aI = joinSpecification.variableToAttributeIndex(v)
      val ref = children(relI).output.filter(a => a.name == (if (aI == 0) "src" else "dst")).head
      ref.withName(v)
    })
  }

  override def references: AttributeSet = AttributeSet(children.flatMap(c => c.output.filter(a => List("src", "dst").contains(a.name))))

  override protected def doExecute(): RDD[InternalRow] = {
    val childRDD = children(0).execute()

    // TODO ask Bogdan if we can enforce that the child needs a specific RDD type
    require(childRDD.isInstanceOf[TrieIterableRDD[TrieIterable]])
    val trieIterableRDD = childRDD.asInstanceOf[TrieIterableRDD[TrieIterable]]


    trieIterableRDD.trieIterables.flatMap(trieIterable => {
      // Do not use create(output, output), then it will use values multiple times in the output
      val toUnsafeProjection = UnsafeProjection.create(output.map(_.dataType).toArray)

      val join = joinSpecification.build(trieIterable)
      val iter = new RowIterator {
        var row: Array[Int]= null
        override def advanceNext(): Boolean = {
          if (join.atEnd) {
            false
          } else {
            row = join.next()
            true
          }
        }

        override def getRow: InternalRow = {
          // TODO can I maybe even safe to construct the generic row?
          val gr = new GenericInternalRow(row.size)
          row.zipWithIndex.foreach { case(b, i) => gr.update(i.toInt, b) }
          toUnsafeProjection(gr)
        }
      }
      iter.toScala
    })
  }
}
