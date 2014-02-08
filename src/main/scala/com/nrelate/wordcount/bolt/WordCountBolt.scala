package com.nrelate.wordcount.bolt

import com.nrelate.wordcount.util.AggregateCount
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple
import com.nrelate.wordcount.helper.TupleHelper._
import com.nrelate.wordcount.util.AnchoredData
import backtype.storm.tuple.Values
import scala.collection.JavaConversions._
import scala.concurrent.duration._

class WordCountBolt(
      outStream: String,
      outStreamFields: Fields,
      override val inputStreamField: String,
      override val interval: FiniteDuration) 
    extends IntervalAggregationBolt[String, AggregateCount] {
  
  type K = String
  type V = AggregateCount

  override def declareOutputFields(declarer: OutputFieldsDeclarer) =
    declarer.declareStream(outStream, outStreamFields)
    
  override def aggregate = (v: V, newval: V) => { v += newval; v }
  
  override def kvPair(tuple: Tuple) =
    tuple.getStringByFieldOpt(inputStreamField) match {
      case Some(word) => Some((word, AggregateCount()))
      case None => None
    }
  
  override def emit(k: K, v: AnchoredData[V]) = 
    collector.emit(outStream, v.anchors, new Values(k, v.data))
}