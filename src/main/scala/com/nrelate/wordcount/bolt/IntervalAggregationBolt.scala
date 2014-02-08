package com.nrelate.wordcount.bolt

import scala.collection.mutable.{Map => MutableMap}
import scala.concurrent.duration.FiniteDuration
import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.tuple.Tuple
import com.nrelate.wordcount.util.AnchoredData
import com.nrelate.wordcount.util.Aggregatable

/** Aggregation bolt which emits after an interval
  *  
  * Extracts value from tuple and upserts into aggregating buffer.  If no
  * value is present in buffer for key, then value is inserted with key.  If
  * tuple contains key {{{k}}} and value {{{{v}}} and buffer contains value
  * {{{agg}}} for {{{k}}}, then {{{upsert}}} calls {{{aggregate(agg, v)}}}
  * 
  * Values of type {{{V}}} should be mutable for performance
  * 
  * This class aggregates using a simple scala {{{Map}}} for now.
  * 
  * @tparam K the tuple's key used for aggregation
  * @tparam V the tuple's value used for aggregation
  * 
  * @author jgraniero
  */
abstract class IntervalAggregationBolt[K, V <: AnyRef]
  extends BaseTimerBolt with Aggregatable[K, V] {
  
  def inputStreamField: String
  
  def interval: FiniteDuration
  
  var buffer: MutableMap[K, AnchoredData[V]]  = _
  
  // flush the buffer after given interval
  override lazy val durationFunctions = Map(interval -> flush)
  
  override def setup(
    conf: java.util.Map[_, _],
    context: TopologyContext,
    outCollector:OutputCollector): Unit = buffer = MutableMap[K, AnchoredData[V]]()
  
  override def process(input: Tuple): Unit = kvPair(input) match {
    case Some((k, v)) => upsert(input, k, v)
    case None => collector.ack(input)
  }
  
  /** This will add the {{{Tuple}}} to the {{{AnchoredData[V]}}} object that
    * already exists in the hashmap.  If no {{{AnchoredData[V]}}} is found
    * in hashmap for key {{{k}}}, then {{{insert(t, k, v)}}} is called and
    * the {{{Tuple}}} is added to the unacked list for the {{{AnchoredData}}}
    * 
    * These tuples are used for anchored emits and must also be
    * ack'd after emit
    */
  private def upsert(t: Tuple, k: K, v: V) = buffer.get(k) match {
      case Some(anchoredData) => aggregate(anchoredData(t).data, v)
      case None => insert(t, k, v)
  }

  /** Creates an {{{AnchoredData[V]}}} instance, inserts into map, and adds
    * {{{Tuple}}} to the list of unacked tuples for the data.
    */
  private def insert(t: Tuple, k: K, v: V) = buffer += k -> AnchoredData(v)(t)
  
  private def flush = () => {
    buffer map { case(k, v) => { emit(k, v); ackAll(v.anchors) } }
    buffer.clear
  }
  
  def get(k: K) = buffer.get(k)
  
  private def ackAll(anchors: Iterable[Tuple]): Unit = anchors foreach collector.ack
  
  def numUnacked = buffer.foldLeft(0)(_ + _._2.anchors.length)
}