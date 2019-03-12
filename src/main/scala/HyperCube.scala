import org.apache.spark._

class Relationship(val attributes: Seq[String]) {

}

class JoinSpecification (val relations: Seq[Relationship]) {
  def allAttributes: Set[String] = {
    Set()
  }
}

/**
  * Currently the user has to duplicate datasets according to getDuplicates and set the partitioner themselves.
  * Then he needs to duplicate all transformations and actions on the datasets and union them in the end.
  *
  * Probably should wrap the datasets completely and offer a dataset like interface to run operations of all
  * duplicated versions of a dataset.
  * @param join
  * @param sizes
  */
class HyperCube (join: JoinSpecification, sizes: Map[String, Int]) {
  val dimensions = join.allAttributes.size

  if (sizes.size != dimensions) {
    throw new Exception("Illegal HyperCube configuration")
  }

  def getDuplicates(rel: Relationship): Int = {
    dimensions - Set(rel.attributes).size + 1
  }

  private def partitions(rel: Relationship): Int = {
    // the number of partitions for rel. Mul(attr in rel.attr, sizes[attr])
    throw new NotImplementedError()
  }

  def getPartitioner(rel: Relationship, duplicate: Int): Partitioner = {
    if (duplicate >= getDuplicates(rel)) {
      throw new Exception(s"This relationship is only duplicated ${getDuplicates(rel)} times not $duplicate.")
    }
    throw new NotImplementedError()
  }
}

class HyperCubePartioner (partitions: Int) extends Partitioner {
  override def numPartitions: Int = {
    partitions
  }

  override def getPartition(key: Any): Int = {
    // Linearizations of an m-cube (number of set attributes)
    throw new NotImplementedError()
  }
}
