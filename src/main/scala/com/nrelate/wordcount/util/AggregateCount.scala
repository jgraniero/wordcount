package com.nrelate.wordcount.util

object AggregateCount {
  def apply(count: Long = 1l) = new AggregateCount(count)
}

class AggregateCount(var count: Long) extends AggregateData[AggregateCount]{

  def +=(that: AggregateCount) = count += that.count
}