package com.nrelate.wordcount.util

import scala.collection.mutable.Buffer
import backtype.storm.tuple.Tuple
import backtype.storm.task.OutputCollector

object AnchoredData {
  def apply[A](data: A) = new AnchoredData[A](data)
}

/** Wrapper for data that is a result of in memory aggregation.  When the
  * aggregate event is emitted, it should be anchored on all of the input
  * tuples used to create it.  
  * 
  * This class helps track which tuples the aggregate data needs to be anchored
  * on for an emit.
  * 
  * @author jgraniero
  */
class AnchoredData[A](val data: A) {

  val anchors = Buffer[Tuple]()
  
  // useful if you know that anchors has at most one item
  def anchor = anchors.head
  
  /** Adds a tuple to the list of anchors for this AggregateData object
    * 
    * @return {{{this}}} for convenience - allows easily adding an anchor right
    *   after creating an instance with the companion object, eg
    *   {{{ AggregateData[Int](100)(tuple) }}}
    */
  def apply(tuple: Tuple): AnchoredData[A]= { anchors += tuple; this }
  
  def numAnchors = anchors.size
  
  def ack(collector: OutputCollector) = anchors foreach { collector.ack }
  
  override def toString: String = data.toString
}