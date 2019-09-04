package leapfrogTriejoin

import org.slf4j.LoggerFactory
import partitioning.shares.Hash
import partitioning.{AllTuples, FirstVariablePartitioningWithWorkstealing, Partitioning, Shares}

import Predef.assert
import util.control.Breaks._
import Predef._


class LeapfrogTriejoin(trieIterators: Map[EdgeRelationship, TrieIterator],
                       variableOrdering: Seq[String],
                       distinctVariables: Boolean = false,
                       smallerThanFilter: Boolean = false,
                       partition: Int = 0,
                       partitioning: Partitioning = AllTuples()
                      ) {

  val logger = LoggerFactory.getLogger(MaterializingLeapfrogJoin.getClass)

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


  private[this] val leapfrogJoins: Array[LeapfrogJoinInterface] = variableOrdering.zipWithIndex.map({ case (v, i) => {
    new MaterializingLeapfrogJoin(
      trieIterators.filter({ case (r, _) => {
        r.variables.contains(v)
      }
      }).values.toArray,
      i,
      partition,
      partitioning
    )
  }
  }).toArray

  partitioning match {
    case p @ FirstVariablePartitioningWithWorkstealing(batchSize) => {
      leapfrogJoins(0) = new WorkstealingLeapfrogjoin(
        FirstVariablePartitioningWithWorkstealing.getQueue(p.queueID), leapfrogJoins(0), batchSize)
    }
    case _ => /* NOP */
  }

  private[this] val variable2TrieIterators: Array[Array[TrieIterator]] = variableOrdering
    .map(v =>
      trieIterators.filter({ case (r, _) => {
        r.variables.contains(v)
      }
      }).values.toArray
    ).toArray

  private[this] var depth = -1
  private[this] var bindings = Array.fill(allVariables.size)(-1L)
  var atEnd: Boolean = trieIterators.values.exists(i => i.atEnd) // Assumes connected join?

  if (!atEnd) {
    moveToNextTuple()
  }

  // TrieIterator used to translate the build tuples before returning them.
  // Any TrieIterator returns the same translation, we choose the first.
  val translator = trieIterators.values.head

  // TODO avoid copying
  def next(): Array[Long] = {
    if (atEnd) {
      throw new IllegalStateException("Cannot call next of LeapfrogTriejoin when already at end.")
    }
    val tuple = new Array[Long](maxDepth + 1) // TODO if I don't use a new array each time this will be faster
    bindings.copyToArray(tuple)
    translator.translate(tuple)
    moveToNextTuple()
    tuple
  }

  private def moveToNextTuple(): Unit = {
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
        leapfrogFilteredNext()
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
    leapfrogFilteredNext()
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
  private def leapfrogFilteredNext(): Unit = {
    if (distinctVariables) {
      while (!currentLeapfrogJoin.atEnd && bindingsContains(currentLeapfrogJoin.key)) {
        currentLeapfrogJoin.leapfrogNext()
      }
    } else if (smallerThanFilter) {
      while (!currentLeapfrogJoin.atEnd && bindingsContainsBigger(currentLeapfrogJoin.key)) {
        currentLeapfrogJoin.leapfrogNext()
      }
    }
  }

  @inline
  private def bindingsContainsBigger(key: Long): Boolean = {
    var i = 0
    var contains = false
    while (i < bindings.length) {
      contains |= bindings(i) >= key
      i += 1
    }
    contains
  }

  @inline
  private def bindingsContains(key: Long): Boolean = {
    var i = 0
    var contains = false
    while (i < bindings.length) {
      contains |= bindings(i) == key
      i += 1
    }
    contains
  }

  @inline
  private def triejoinOpen(): Unit = {
    depth += 1

    val trieIterators = variable2TrieIterators(depth)
    var i = 0
    while (i < trieIterators.length) {
      trieIterators(i).open()
      i += 1
    }

    leapfrogJoins(depth).init()
  }

  @inline
  private def triejoinUp(): Unit = {
    val trieIterators = variable2TrieIterators(depth)
    var i = 0
    while (i < trieIterators.length) {
      trieIterators(i).up()
      i += 1
    }

    bindings(depth) = -1
    depth -= 1
  }

  @inline
  private def currentLeapfrogJoin: LeapfrogJoinInterface = {
    leapfrogJoins(depth)
  }
}
