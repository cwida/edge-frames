package leapfrogTriejoin

abstract class LeapfrogJoinInterface {

  def init(): Unit
  def leapfrogNext(): Unit
  def key: Long
  def atEnd: Boolean
}
