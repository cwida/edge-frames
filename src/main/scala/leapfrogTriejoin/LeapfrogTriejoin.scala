package leapfrogTriejoin
import Predef.assert
import util.control.Breaks._
import Predef._


class LeapfrogTriejoin(trieIterators: Map[EdgeRelationship, TrieIterator], variableOrdering: Seq[String]) {

  val allVariables = trieIterators.keys.flatMap(
    e => e.variables).toSet

  require(allVariables == variableOrdering.toSet,
    s"The set of all variables in the relationships needs to equal the variable ordering. All variables: $allVariables, variableOrdering: $variableOrdering"
  )

  require(trieIterators.keys
    .forall(r => {
      val relevantVars = variableOrdering.filter(v => r.variables.contains(v)).toList
      relevantVars == relevantVars.sortBy(v => r.variables.indexOf(v))
    }),
    "Variable ordering differs for some relationships."
  )

  val leapfrogJoins = allVariables
    .map(v =>
        (v, new LeapfrogJoin(
          trieIterators.filter({ case (r, _) => r.variables.contains(v) }).map(_._2).toArray)))
    .toMap

  var depth = -1
  var bindings = Array.fill(allVariables.size)(-1)
  var atEnd = trieIterators.values.exists(i => i.atEnd)  // Assumes connected join?

  if (!atEnd) {
    moveToNextTuple()
  }

  def next(): Array[Int] = {
    if (atEnd) {
      throw new IllegalStateException("Cannot call next of LeapfrogTriejoin when already at end.")
    }
    val tuple = bindings.clone()
    moveToNextTuple()

    tuple
  }



  private def moveToNextTuple() = {
    val DOWN_ACTION: Int = 0
    val NEXT_ACTION: Int = 1
    val UP_ACTION: Int = 2

    var action: Int = NEXT_ACTION
    if (depth == -1) {
      action = DOWN_ACTION
    } else if (currentLeapfrogJoin.atEnd) {
      action = UP_ACTION
    }
    var done = false
    while (!done) {
      if (action == NEXT_ACTION) {
        currentLeapfrogJoin.leapfrogNext()
        if (currentLeapfrogJoin.atEnd) {
          action = UP_ACTION
        } else {
          bindings(depth) = currentLeapfrogJoin.key
          if (depth == allVariables.size - 1) {
            done = true
          } else {
            action = DOWN_ACTION
          }
        }
      } else if (action == DOWN_ACTION) {
        triejoinOpen()
        if (currentLeapfrogJoin.atEnd) {
          action = UP_ACTION
        } else {
          bindings(depth) = currentLeapfrogJoin.key

          if (depth == allVariables.size - 1) {
            done = true
          } else {
            action = DOWN_ACTION
          }
        }
      } else if (action == UP_ACTION) {
        if (depth == 0) {
          done = true
          atEnd = true
        } else {
          triejoinUp()
          if (currentLeapfrogJoin.atEnd) {
            action = UP_ACTION
          } else {
            action = NEXT_ACTION
          }
        }
      }
    }
  }
  private def triejoinOpen() ={
    depth += 1
    val variable = variableOrdering(depth)
    trieIterators
      .filter( { case (r, _) => r.variables.contains(variable) })
      .foreach( { case (_, i) => i.open() })
    leapfrogJoins(variable).init()
  }

  private def triejoinUp() = {
    trieIterators
      .filter( { case (r, _) => r.variables.contains(variableOrdering(depth)) })
      .foreach( { case (r, i) => i.up() })
    bindings(depth) = -1
    depth -= 1
  }

  private def currentLeapfrogJoin: LeapfrogJoin = {
    leapfrogJoins(variableOrdering(depth))
  }
}
