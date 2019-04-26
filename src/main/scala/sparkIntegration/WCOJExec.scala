package sparkIntegration

import leapfrogTriejoin.TrieIterable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.{RowIterator, SparkPlan}
import sparkIntegration.implicits._

import scala.reflect.ClassTag

case class WCOJExec(outputVariables: Seq[Attribute], joinSpecification: JoinSpecification, children: Seq[SparkPlan]) extends SparkPlan {

  override def output: Seq[Attribute] = {
    outputVariables
  }

  override def references: AttributeSet = AttributeSet(children.flatMap(c => c.output.filter(a => List("src", "dst").contains(a.name))))

  override protected def doExecute(): RDD[InternalRow] = {
    val childRDDs = children.map(_.execute())

    // TODO ask Bogdan if we can enforce that the child needs a specific RDD type
    require(childRDDs.forall(_.isInstanceOf[TrieIterableRDD[TrieIterable]]))

    val trieIterableRDDs = childRDDs.map(_.asInstanceOf[TrieIterableRDD[TrieIterable]].trieIterables)

    def zipPartitions(is : List[Iterator[TrieIterable]]): Iterator[InternalRow] = {
      val toUnsafeProjection = UnsafeProjection.create(output.map(_.dataType).toArray)

      val zippedIters: Iterator[List[TrieIterable]] = generalZip(is)

      zippedIters.flatMap( a => {
        val join = joinSpecification.build(a)
        val iter = new RowIterator {
          var row: Array[Int] = null

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
            row.zipWithIndex.foreach { case (b, i) => gr.update(i.toInt, b) }
            toUnsafeProjection(gr)
          }
        }
        iter.toScala
      }
      )
    }

    trieIterableRDDs match {
      case Nil => throw new UnsupportedOperationException("Cannot join without any child.")
      case c1 :: Nil => c1.mapPartitions(i => zipPartitions(List(i)))
      case c1 :: c2 :: t => generalZipPartitions(c1 :: c2 :: t)(zipPartitions)
      case _ => throw new UnsupportedOperationException("Currently, due to Sparks limited zipping functionality we do not support WCOJ joins with more than 4 children.")
    }
  }

  private def generalZip[A](s : List[Iterator[A]]): Iterator[List[A]] = s match {
    case Nil => Iterator.empty
    case h1 :: Nil => h1.map(_ :: Nil)
    case h1 :: h2 :: Nil => h1.zip(h2).map( { case (l, r) => List(l, r)})
    case h1 :: t => h1.zip(generalZip(t)).map ( { case (l, r) => l :: r})
  }

  private def generalZipPartitions[A: ClassTag, V: ClassTag](rdds: List[RDD[A]])(f: (List[Iterator[A]]) => Iterator[V]): RDD[V] = {
    rdds.head.generalZippedPartitions(sparkContext, rdds.tail)(f)
  }
}
