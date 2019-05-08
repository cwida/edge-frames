package leapfrogTriejoin

class LeapfrogJoin(var iterators: Array[LinearIterator]) {
  if (iterators.isEmpty) {
    throw new IllegalArgumentException("iterators cannot be empty")
  }

  var atEnd: Boolean = false
  private[this] var p = 0
  var key = 0

  def init(): Unit = {
    iteratorAtEndExists()

    p = 0
    key = 0

    if (!atEnd) {
      sortIterators()
      leapfrogSearch()
    }
  }

  @inline
  private def iteratorAtEndExists(): Unit = {
    atEnd = false
    var i = 0
    while (i < iterators.length) {
      if (iterators(i).atEnd) {
        atEnd = true
      }
      i += 1
    }
  }

  // Public for testing
  def sortIterators(): Unit = {
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
    var max = iterators(if (p > 0) {
      p - 1
    } else {
      iterators.length - 1
    }).key

    var min = iterators(p).key

    while (min != max && !iterators(p).seek(max)) {
      max = iterators(p).key

      if (p < iterators.length - 1) {
        p += 1
      } else {
        p = 0
      }
      min = iterators(p).key
    }
    key = min
    atEnd = iterators(p).atEnd
  }

  def leapfrogNext(): Unit = {
    iterators(p).next()
    if (iterators(p).atEnd) {
      atEnd = true
    } else {
      if (p < iterators.length - 1) {
        p += 1
      } else {
        p = 0
      }
      leapfrogSearch()
    }
  }

  def leapfrogSeek(key: Int): Unit = {
    iterators(p).seek(key)
    if (iterators(p).atEnd) {
      atEnd = true
    } else {
      if (p < iterators.length - 1) {
        p += 1
      } else {
        p = 0
      }
      leapfrogSearch()
    }
  }
}
