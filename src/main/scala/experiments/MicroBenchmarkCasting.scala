package experiments

object MicroBenchmarkCasting extends App {
  val reps = 1000000
  val values = (0 until reps).toArray

  var i = 0
  var warmUp = 0
  while (i < reps) {
    warmUp += returnInt(values(i))
    i += 1
  }

  i = 0
  var sum = 0
  val startI = System.nanoTime()
  while (i < reps) {
    sum += returnInt(i)
    i += 1
  }
  val endI = System.nanoTime()


  var sumL = 0L
  val startL = System.nanoTime()
  var iL = 0L
  while (iL < reps) {
    sumL += returnLong(iL)
    iL += 1
  }
  val endL = System.nanoTime()

  println("I: ", (endI - startI).toDouble / 1000000000, sum)
  println("L: ", (endL - startL).toDouble / 1000000000, sumL)

  def returnInt(i: Int): Int = values(i)
  def returnLong(i: Long): Long = values(i.toInt)

}
