package testing

import java.io.File
import java.net.InetAddress

import leapfrogTriejoin.TrieIterator
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import sparkIntegration.{ToTrieIterableRDD2ToTrieIterableRDDExec, WCOJ2WCOJExec, WCOJConfiguration}

import scala.collection.mutable
import scala.reflect.ClassTag

object Utils {

  def traverseTrieIterator(iter: TrieIterator): Seq[(Long, Long)] = {
    if (iter.atEnd) {
      return List()
    }
    var ret: mutable.MutableList[(Long, Long)] = mutable.MutableList()
    iter.open()
    do {
      val outer: Long = iter.key
      iter.open()
      do {
        ret += ((outer, iter.key))
        iter.next()
      } while (!iter.atEnd)
      iter.up()
      iter.next()
    } while (!iter.atEnd)
    ret
  }

  // Stackoverflow: https://stackoverflow.com/questions/25999255/delete-directory-recursively-in-scala
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteRecursively)
    }
    if (file.exists && !file.delete) {
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }
  }

  def printSetRDD[A: ClassTag](n: Int, rdd: RDD[Set[A]]): Unit = {
    println(rdd.take(n).map(s => s.mkString(", ")).mkString("\n"))
    val size = rdd.count()
    if (size > n) {
      println(s"Showing only $n out of $size rows.")
    }
  }

  def printSeqRDD[A: ClassTag](n: Int, rdd: RDD[Seq[A]]): Unit = {
    println(rdd.take(n).map(s => s.mkString(", ")).mkString("\n"))
    val size = rdd.count()
    if (size > n) {
      println(s"Showing only $n out of $size rows.")
    }
  }

  def getDatasetPath(datasetName: String): String = {
    InetAddress.getLocalHost.getHostName match {
      case "bluefox" => "/home/per/workspace/master-thesis/datasets/" + datasetName
      case "bricks02.scilens.private" => "file:///scratch/per/datasets/" + datasetName
      case s => throw new IllegalStateException("Unknown dataset path location for hostname: " + s)
    }
  }

  def getQueryCachePath: String = {
    InetAddress.getLocalHost.getHostName match {
      case "bluefox" => "./queryCache"
      case "bricks02.scilens.private" => "file:///scratch/per/query-cache/"
      case s => throw new IllegalStateException("Unknown dataset path location for hostname: " + s)
    }
  }

}

object TestSparkSession {
  val conf = new SparkConf()
    .setMaster("local[1]")
    .setAppName("Spark test")
    .set("spark.executor.memory", "40g")
    .set("spark.driver.memory", "40g")
    .set("spark.sql.autoBroadcastJoinThreshold", "1048576000") // High threshold


  val spark = SparkSession.builder().config(conf).getOrCreate()

  WCOJConfiguration(spark)

  spark.experimental.extraStrategies = Seq(ToTrieIterableRDD2ToTrieIterableRDDExec, WCOJ2WCOJExec) ++ spark.experimental.extraStrategies
}

trait SparkTest {
  val sp = TestSparkSession.spark
}

