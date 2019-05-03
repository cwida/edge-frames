package leapfrogTriejoin

class LeapfrogJoin(var iterators: Array[LinearIterator] ) {
  if (iterators.isEmpty) {
    throw new IllegalArgumentException("iterators cannot be empty")
  }

  var atEnd: Boolean = false
  var p = 0
  var key = 0

  def init(): Unit = {
    atEnd = iterators.exists(i => i.atEnd)
    p = 0
    key = 0

    if (!atEnd) {
      sortIterators()
      leapfrogSearch()
    }
  }

  // Public for testing

  // TODO look at ways to avoid that sort.
  def sortIterators(): Unit = {
//    iterators = iterators.sortBy(i => i.key)
    var i = 1
    while (i < iterators.size) {
      val iteratorToSort = iterators(i)
      val keyToSort = iterators(i).key
      var j = i
      while (j > 0 && iterators(j - 1).key > keyToSort) {
        iterators(j) = iterators(j - 1)
        j -= 1
      }
      iterators(j) = iteratorToSort
      i += 1
    }
  }

  private def leapfrogSearch(): Unit = {
    var max = iterators(if (p > 0) { p - 1 } else {iterators.length - 1}).key
    while (true) {
      var min = iterators(p).key
      if (min == max) {
        key = min
        return
      } else {
        iterators(p).seek(max)
        if (iterators(p).atEnd) {
          atEnd = true
          return
        } else {
          max = iterators(p).key
          p = (p + 1) % iterators.length
        }
      }
    }
  }

  def leapfrogNext(): Unit = {
    iterators(p).next()
    if (iterators(p).atEnd) {
      atEnd = true
    } else {
      p = (p + 1) % iterators.length
      leapfrogSearch()
    }
  }

  def leapfrogSeek(key: Int): Unit = {
    iterators(p).seek(key)
    if (iterators(p).atEnd) {
      atEnd = true
    } else {
      p = (p + 1) % iterators.length
      leapfrogSearch()
    }
  }
}
