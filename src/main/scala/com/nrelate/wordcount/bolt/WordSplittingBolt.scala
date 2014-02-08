package com.nrelate.wordcount.bolt

import backtype.storm.topology.base.BaseRichBolt
import java.util.{Map => JMap}
import backtype.storm.task.TopologyContext
import backtype.storm.task.OutputCollector
import backtype.storm.tuple.Tuple
import com.nrelate.wordcount.helper.TupleHelper._
import backtype.storm.tuple.Values
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.Fields
import scala.util.matching.Regex

object WordSplittingBolt {
  val WordSplit = """\w+""".r
  val OutStream = "words"
  val OutField  = "word"
    
  def apply(
      wordSplit: Regex 	= WordSplit,
      stream: String 		= OutStream,
      streamField: String	= OutField) = 
    new WordSplittingBolt(wordSplit, stream, streamField)
}

class WordSplittingBolt(
    wordSplit: Regex,
    stream: String,
    streamField: String) extends BaseRichBolt {
  
  import WordSplittingBolt._
  
  var collector: OutputCollector = _

  override def prepare(
    stormConf: JMap[_, _],
    context: TopologyContext,
    outCollector: OutputCollector) = collector = outCollector
    
  override def execute(tuple: Tuple) = 
    if (!tuple.isTickTuple) {
      tuple.getStringOpt(0) match {
        case Some(str) => 
          wordSplit.split(str) foreach { word =>
            collector.emit(stream, tuple, new Values(word))
          }
        case None => 
      }
    }
  
  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit =
    declarer.declareStream(stream, new Fields(streamField))
}