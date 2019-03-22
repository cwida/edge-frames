package leapfrogTriejoin
import Predef.assert
import util.control.Breaks._
import Predef._


class LeapfrogTriejoin(trieIterators: Seq[TrieIterator], variableOrdering: Seq[String]) {
  val allVariables = trieIterators.flatMap(
    t => t.relationship.variables).toSet

  assert(allVariables == variableOrdering.toSet,
    s"The set of all variables in the relationships needs to equal the variable ordering. All variables: $allVariables, variableOrdering: $variableOrdering"
  )

  assert(trieIterators
    .forall(i => {
      val relevantVars = variableOrdering.filter(v => i.relationship.variables.contains(v)).toList
      relevantVars == relevantVars.sortBy(v => i.relationship.variables.indexOf(v))
    }),
    "Variable ordering differs for some relationships."
  )

  val leapfrogJoins = allVariables
    .map(v =>
        (v, new LeapfrogJoin(
          trieIterators.filter(i => i.relationship.variables.contains(v)).toArray)))
    .toMap

  var depth = -1
  var bindings = Array.fill(allVariables.size)(-1)
  var atEnd = trieIterators.exists(i => i.atEnd)  // Assumes connected join?

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
      .filter(i => i.relationship.variables.contains(variable))
      .foreach(i => i.open())
    leapfrogJoins(variable).init()
  }

  private def triejoinUp() = {
    trieIterators
      .filter(i => i.relationship.variables.contains(variableOrdering(depth)))
      .foreach(i => i.up())
    bindings(depth) = -1
    depth -= 1
  }

  private def currentLeapfrogJoin: LeapfrogJoin = {
    leapfrogJoins(variableOrdering(depth))
  }
}
