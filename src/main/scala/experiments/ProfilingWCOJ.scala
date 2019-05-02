package experiments

import experiments.ProfilingWCOJ.join
import leapfrogTriejoin.{ArrayTrieIterable, EdgeRelationship, LeapfrogTriejoin, TrieIterator}
import leapfrogTriejoin.implicits._

import scala.collection.mutable.ListBuffer
import scala.io.Source

object ProfilingWCOJ extends App {

  val DATASET_PATH = "/home/per/workspace/master-thesis/datasets/amazon-0302.csv"

  val ds: Array[(Int, Int)] = loadDataset(DATASET_PATH)

  val edges: List[EdgeRelationship] = ('a' to 'c')
    .combinations(2)
    .filter(l => l(0) < l(1))
    .map(l => new EdgeRelationship((s"${l(0)}", s"${l(1)}")))
    .toList

  val rels: List[TrieIterator] = edges
    .map(e => new ArrayTrieIterable(ds).trieIterator)

  val join = new LeapfrogTriejoin(edges.zip(rels).toMap, Seq("a", "b", "c"))

  doJoin(join)

  def doJoin(join: LeapfrogTriejoin) = {
    val start = System.nanoTime()
    val result = join.toList
    val end = System.nanoTime()
    println(s"Result size: ${result.size}, ${(end - start).toDouble / 1000000000}")
  }

  def loadDataset(datasetPath: String): Array[(Int, Int)] = {
    val output = new ListBuffer[(Int, Int)]()
    val bufferedSource = Source.fromFile(datasetPath)
    for (line <- bufferedSource.getLines) {
      if (!line.startsWith("#")) {
        val cols = line.split("\t")
        require(cols.size == 2)
        output.append((cols(0).toInt, cols(1).toInt))
      }
    }
    bufferedSource.close
    output.toArray
  }

}
