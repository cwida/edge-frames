package leapfrogTriejoin

class LeapfrogJoin(iterators: Array[UnaryIterator] ) {
  if (iterators.isEmpty) {
    throw new IllegalArgumentException("iterators cannot be empty")
  }

  var atEnd: Boolean = false
  var p = 0
  var key = 0

  def init(): Unit = {
    if (iterators.exists(i => i.atEnd)) {
      atEnd = true
    } else {
      iterators.sortBy(i => i.key)
      p = 0
      leapfrogSearch()
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
