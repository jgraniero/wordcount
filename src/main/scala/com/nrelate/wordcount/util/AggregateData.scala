package com.nrelate.wordcount.util

/** AggregateData defines one function, {{{+=}}} which defines how to combine
  * itself with another instance of the same type
  */
trait AggregateData[T <: AggregateData[T]] { def +=(that: T) }