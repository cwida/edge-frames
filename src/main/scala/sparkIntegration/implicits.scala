package sparkIntegration

import org.apache.spark.rdd.{RDD, RDDFunctions}
import org.apache.spark.sql.{Dataset, WCOJFunctions}

import scala.reflect.ClassTag

object implicits {
  import scala.language.implicitConversions

  implicit def datasetToCachedGraphTopologyFunctions[T](ds: Dataset[T]): WCOJFunctions[T] =
    new WCOJFunctions[T](ds)

  implicit def RDD2RDDFunctions[T: ClassTag](rdd: RDD[T]): RDDFunctions[T] = new RDDFunctions[T](rdd)
}
