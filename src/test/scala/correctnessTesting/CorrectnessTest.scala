package correctnessTesting

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}
import testing.{SparkTest, Utils}

class CorrectnessTest extends FlatSpec with Matchers with SparkTest {

  def assertRDDEqual(a: RDD[Row], e: RDD[Row]) = {
    val aExtras = a.subtract(e)
    val eExtras = e.subtract(a)

    val aExtrasEmpty = aExtras.isEmpty()
    val eExtrasEmpty = eExtras.isEmpty()

    if (!(aExtrasEmpty && eExtrasEmpty)) {
      Utils.printSeqRDD(50, aExtras.map(r => r.toSeq))
      Utils.printSeqRDD(50, eExtras.map(r => r.toSeq))
    }

    aExtrasEmpty should be(true)
    eExtrasEmpty should be(true)
  }

  def assertRDDSetEqual(rdd1: RDD[Row], rdd2: RDD[Row], setSize: Int) = {
    val rdd1Set = rdd1.map(r => r.toSeq.toSet)
    val rdd2Set = rdd2.map(r => r.toSeq.toSet)

    rdd1Set.filter(_.size != setSize).isEmpty() should be (true)
    rdd2Set.filter(_.size != setSize).isEmpty() should be (true)


    val diff1 = rdd1Set.subtract(rdd2Set)
    val diff2 = rdd2Set.subtract(rdd1Set)

    val empty1 = diff1.isEmpty()
    val empty2 = diff2.isEmpty()

    if (!(empty1 && empty2)) {
      Utils.printSetRDD(50, diff1)
      Utils.printSetRDD(50, diff2)
    }

    empty1 should be (true)
    empty2 should be (true)
  }

}
