package sharesSimulator

object Utils {

  def cartesian(seq: Seq[Seq[Int]]): Seq[List[Int]] = seq match {
    case Nil => List(Nil)
    case x :: xs => x.flatMap(v => cartesian(xs).map(ls => v :: ls))
  }

}
