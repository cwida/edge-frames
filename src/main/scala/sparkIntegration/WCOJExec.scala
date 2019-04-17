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
      new AttributeReference(v, IntegerType, false)(ref.exprId)
    })
  }

  override def references: AttributeSet = AttributeSet(children.flatMap(c => c.output.filter(a => List("src", "dst").contains(a.name))))

  override protected def doExecute(): RDD[InternalRow] = {
    val childRDD = children(0).execute()

    // TODO ask Bogdan if we can enforce that the child needs a specific RDD type
    require(childRDD.isInstanceOf[TrieIterableRDD[TrieIterable]])
    val trieIterableRDD = childRDD.asInstanceOf[TrieIterableRDD[TrieIterable]]

    trieIterableRDD.trieIterables.flatMap(trieIterable => {
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
          val gr = new GenericInternalRow(row.size)
          row.zipWithIndex.foreach { case(b, i) => gr.update(i.toInt, b) }
//          toUnsafeRow(gr, Array(IntegerType, IntegerType))  // TODO Bogdan without this is throws a class cast exception when I collect the result, with it it's super slow.
          gr
        }
      }
      iter.toScala
    })
  }

  private def toUnsafeRow(row: InternalRow, schema: Array[DataType]): UnsafeRow = {
    val converter = unsafeRowConverter(schema)
    converter(row)
  }

  private def unsafeRowConverter(schema: Array[DataType]): InternalRow => UnsafeRow = {
    val converter = UnsafeProjection.create(schema)
    row: InternalRow => {
      converter(CatalystTypeConverters.convertToCatalyst(row).asInstanceOf[InternalRow])
    }
  }
}
