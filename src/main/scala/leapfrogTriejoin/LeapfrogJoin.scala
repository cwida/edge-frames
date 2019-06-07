package leapfrogTriejoin

class LeapfrogJoin(var iterators: Array[LinearIterator]) {
  if (iterators.isEmpty) {
    throw new IllegalArgumentException("iterators cannot be empty")
  }

  var atEnd: Boolean = false
  private[this] var p = 0
  var key = 0L

  def init(): Unit = {
    iteratorAtEndExists()

    p = 0
    key = -1

    if (!atEnd) {
//      println(iterators.map(_.estimateSize).mkString(", "))
      sortIterators()
      if (!atEnd && key == -1) {
        leapfrogSearch()
      }
    }
//    println(atEnd)
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

  // sorts the iterator such that p points to the smallest iterator and p - 1 to the largest
  // Public for testing, returns p for testing
  def sortIterators(): Int = {
    var i = 0
    var max = 0L
    var maxPosition = 0
    while (i < iterators.length) {
      val key = iterators(i).key
      if (key > max) {
        max = key
        maxPosition = i
      }
      i += 1
    }

    p = if (maxPosition + 1 < iterators.length) maxPosition + 1 else 0
    while (p != maxPosition && !iterators(p).seek(max)) {
      max = iterators(p).key
      p = if (p + 1 < iterators.length) p + 1 else 0
    }

    atEnd = iterators(p).atEnd
    if (!atEnd && iterators(p).key == max) {
      key = max
    }
    p
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

  def leapfrogSeek(key: Long): Unit = {
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
