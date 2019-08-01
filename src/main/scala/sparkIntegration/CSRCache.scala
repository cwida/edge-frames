package sparkIntegration

import leapfrogTriejoin.{CSRTrieIterable, TrieIterable}
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable

object CSRCache {

  private val cache: mutable.Map[Long, Broadcast[(TrieIterable, TrieIterable)]] = new mutable.HashMap()

  def put(graphID: Long, trieIterables: Broadcast[(TrieIterable, TrieIterable)]): Unit = {
    cache.put(graphID, trieIterables)
  }

  def get(graphID: Long): Option[Broadcast[(TrieIterable, TrieIterable)]] = {
    cache.get(graphID)
  }

}
