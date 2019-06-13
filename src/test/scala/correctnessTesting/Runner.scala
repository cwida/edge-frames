package correctnessTesting
import org.scalatest._

object Runner extends App {
  CorrectnessTest.FAST = false
  run(new AmazonGraphWCOJ)
  run(new GoogleWebGraphWCOJ)
  run(new SNBGraphWCOJ)

  run(new SNBWCOJ)
  run(new AmazonWCOJ)
  run(new GoogleWebWCOJ)

}
