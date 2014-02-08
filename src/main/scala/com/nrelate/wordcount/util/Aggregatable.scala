package com.nrelate.wordcount.util

import backtype.storm.tuple.Tuple
import backtype.storm.task.OutputCollector
import backtype.storm.tuple.Values
import scala.collection.JavaConversions._

/** A trait that describes data which can be aggregated
  * 
  * Aggregate data is usually stored in some sort of (key, value) map.  This
  * trait contains the methods used by tempest-common to aggregate data.
  * 
  * {{{kvPair(t: Tuple)}}} defines how the (key, value) pair is extracted from 
  * the {{{Tuple}}}
  * 
  * {{{aggregate: (V, V) => V}}} defines how two instances of {{{V}}} are combined
  * to form an aggregate {{{V}}}j
  * 
  * {{{emit(k: K, v: AnchoredData[V])}}} defines how the (key, value) pair is
  * reassembled for the emit
  * 
  * @author jgraniero
  */
trait Aggregatable[K, V <: AnyRef] {
  
  /** Since {{{Aggregatable}}} forces the implementing class to define {{{emit}}},
    * it should also have access to the {{{collector}}} so that it can actually...
    * ...emit...
    */
  def collector: OutputCollector
  
  /** Defines how to emit the aggregate data.  Useful for transforming the
    * (key, value) pair to something possibly more useful for the downstream
    * bolt(s)
    */
  def emit(k: K, v: AnchoredData[V]): Unit

  /** Aggregate data has a key and a value.  This function defines how to extract
    * a (key, value) pair from a {{{Tuple}}} 
    */
  def kvPair(t: Tuple): Option[(K, V)]
  
  /** Defines how to combine, or aggregate, two instances of {{{V}}}
    */
  def aggregate: (V, V) => V
}