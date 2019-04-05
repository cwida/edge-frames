package scalaIntegration

import org.apache.spark.sql.{Dataset}

object implicits {
  import scala.language.implicitConversions

  implicit def datasetToCachedGraphTopologyFunctions[T](ds: Dataset[T]): WCOJFunctions[T] =
    new WCOJFunctions[T](ds)
}
