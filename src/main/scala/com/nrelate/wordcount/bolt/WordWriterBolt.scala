package com.nrelate.wordcount.bolt

import scala.concurrent.duration._
import javax.sql.DataSource
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.Tuple
import java.sql.PreparedStatement
import com.nrelate.wordcount.helper.TupleHelper._

object WordWriterBolt {
  val SQLTable = "word_count"
  val Query = 
    s"INSERT INTO $SQLTable (word, count) VALUES (?, ?) " +
     "ON DUPLICATE KEY UPDATE word = word+?"
    
  def apply(
      kvFields: (String, String),
      interval: FiniteDuration,
      mysql: DataSource,
      batchSize: Int = MysqlWriterBolt.DefaultBatchSize) =
    new WordWriterBolt(Query, kvFields, interval, batchSize, mysql)
}

class WordWriterBolt(
    query: String,
    kvFields: (String, String),
    interval: FiniteDuration,
    batchSize: Int,
    ds: DataSource) extends MysqlWriterBolt(query, batchSize, interval) {
  
  override def connectDataSource = ds
  
  override def declareOutputFields(declarer: OutputFieldsDeclarer) = {}
  
  override def bind(stmt: PreparedStatement, tuple: Tuple) = 
    extractPair(tuple) match {
      case (Some(word), Some(count)) =>
        stmt.setLong(1, count.toLong)
        stmt.setLong(2, count.toLong)
        true
      case _ =>  false
    }
  
  private def extractPair(tuple: Tuple) = {
    val key = tuple.getStringByFieldOpt(kvFields._1)
    val value = tuple.getValueByFieldAs[Long](kvFields._2)
    
    (key, value)
  }
}