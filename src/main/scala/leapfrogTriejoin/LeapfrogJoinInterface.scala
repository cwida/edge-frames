package leapfrogTriejoin

abstract class LeapfrogJoinInterface {

  def init(): Unit
  def leapfrogNext(): Unit
  def leapfrogSeek(key: Long): Unit
  def key: Long
  def atEnd: Boolean
}
