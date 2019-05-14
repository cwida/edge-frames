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

  def assertRDDSetEqual(a: RDD[Row], e: RDD[Row], setSize: Int) = {
    val aSet = a.map(r => r.toSeq.toSet)
    val eSet = e.map(r => r.toSeq.toSet)

    aSet.filter(_.size != setSize).isEmpty() should be (true)
    eSet.filter(_.size != setSize).isEmpty() should be (true)


    val aExtra = aSet.subtract(eSet)
    val eExtra = eSet.subtract(aSet)

    val empty1 = aExtra.isEmpty()
    val empty2 = eExtra.isEmpty()

    if (!(empty1 && empty2)) {
      println("actual contains following extra rows: ")
      Utils.printSetRDD(50, aExtra)
      println("expected contains following extra rows: ")
      Utils.printSetRDD(50, eExtra)
    }

    empty1 should be (true)
    empty2 should be (true)
  }

}
