package experiments.metrics

class LocalTimer {
  val start = System.nanoTime

  /**
    * @return Time since object creation in microseconds
    */
  def stop: Double = {
    (System.nanoTime() - start) / 1e6
  }
}
