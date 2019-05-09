package leapfrogTriejoin

import Predef.assert
import util.control.Breaks._
import Predef._


class LeapfrogTriejoin(trieIterators: Map[EdgeRelationship, TrieIterator], variableOrdering: Seq[String], distinctVariables: Boolean = false) {

  private[this] val DONE: Int = 0
  private[this] val DOWN_ACTION: Int = 1
  private[this] val NEXT_ACTION: Int = 2
  private[this] val UP_ACTION: Int = 3

  val allVariables = trieIterators.keys.flatMap(
    e => e.variables).toSet

  private[this] val maxDepth = allVariables.size - 1

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

  private[this] val leapfrogJoins: Array[LeapfrogJoin] = variableOrdering
    .map(v =>
      new LeapfrogJoin(trieIterators
        .filter({ case (r, _) => {
          r.variables.contains(v)
        }
        }).values.toArray))
    .toArray

  private[this] val variable2TrieIterators: Array[Array[TrieIterator]] = variableOrdering
    .map(v =>
      trieIterators.filter({ case (r, _) => {
        r.variables.contains(v)
      }
      }).values.toArray
    ).toArray

  private[this] var depth = -1
  private[this] var bindings = Array.fill(allVariables.size)(-1)
  var atEnd = trieIterators.values.exists(i => i.atEnd) // Assumes connected join?

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
    var action: Int = NEXT_ACTION
    if (depth == -1) {
      action = DOWN_ACTION
    } else if (currentLeapfrogJoin.atEnd) {
      action = UP_ACTION
    }
    if (action == NEXT_ACTION) {
      action = nextAction()
    }

    while (action != DONE) {
      if (action == DOWN_ACTION) {
        triejoinOpen()
        if (distinctVariables) {
          leapfrogDistinctNext()
        }
        if (currentLeapfrogJoin.atEnd) {
          action = UP_ACTION
        } else {
          bindings(depth) = currentLeapfrogJoin.key

          if (depth == maxDepth) {
            action = DONE
          } else {
            action = DOWN_ACTION
          }
        }
      } else if (action == UP_ACTION) {
        if (depth == 0) {
          action = DONE
          atEnd = true
        } else {
          triejoinUp()
          if (currentLeapfrogJoin.atEnd) {
            action = UP_ACTION
          } else {
            action = nextAction()
          }
        }
      }
    }
  }

  @inline
  private def nextAction(): Int = {
    currentLeapfrogJoin.leapfrogNext()
    if (distinctVariables) {
      leapfrogDistinctNext()
    }
    if (currentLeapfrogJoin.atEnd) {
      UP_ACTION
    } else {
      bindings(depth) = currentLeapfrogJoin.key
      if (depth == maxDepth) {
        DONE
      } else {
        DOWN_ACTION
      }
    }

  }

  @inline
  private def leapfrogDistinctNext(): Unit = {
    while (!currentLeapfrogJoin.atEnd && bindingsContains(currentLeapfrogJoin.key)) {
      currentLeapfrogJoin.leapfrogNext()
    }
  }

  @inline
  private def bindingsContains(key: Int): Boolean = {
    var i = 0
    var contains = false
    while (i < bindings.length) {
      contains |= bindings(i) == key
      i += 1
    }
    contains
  }

  @inline
  private def triejoinOpen() = {
    depth += 1

    whileForeach(variable2TrieIterators(depth), _.open())

    leapfrogJoins(depth).init()
  }

  @inline
  private def triejoinUp() = {
    whileForeach(variable2TrieIterators(depth), _.up())

    bindings(depth) = -1
    depth -= 1
  }


  @inline // Faster than Scala's foreach because it actually gets inlined
  private def whileForeach(ts: Array[TrieIterator], f: TrieIterator => Unit): Unit = {
    var i = 0
    while (i < ts.length) {
      f(ts(i))
      i += 1
    }
  }

  @inline
  private def currentLeapfrogJoin: LeapfrogJoin = {
    leapfrogJoins(depth)
  }
}
