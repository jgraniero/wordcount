package com.nrelate.wordcount.bolt

import scala.concurrent.duration._
import com.nrelate.wordcount.util.MySQL
import java.sql.PreparedStatement
import javax.sql.DataSource
import backtype.storm.tuple.Tuple
import scala.collection.mutable.ListBuffer
import backtype.storm.task.TopologyContext
import backtype.storm.task.OutputCollector
import java.sql.BatchUpdateException
import java.sql.SQLException

object MysqlWriterBolt {
  val DefaultBatchSize = 300
}

/** Bolt for writing data to MySQL on an interval
  *  
  * This bolt will create a list of events until {{{flush()}}} is called.
  * {{{flush()}}} will write to MySQL in batches defined by {{{batchSize}}}
  * 
  * @tparam A the type of data from which values will be extracted and bound
  *   to the MySQL {{{PreparedStatement}}}
  * 
  * @author jgraniero
  */
abstract class MysqlWriterBolt(
    query: String,
    batchSize: Int,
    interval: FiniteDuration)
  extends BaseTimerBolt with MySQL {
  
  /** The {{{DataSource}}} used for creating {{{Connection}}}s
    */
  def connectDataSource: DataSource
  
  /** Defines how values from {{{data}}} are bound to the {{{stmt}}}
    *  
    * @return {{{true}}} if values successfully extracted from tuple, {{{false}}}
    *   otherwise
    * 
    * @see [[java.sq.PreparedStatement]]
    */
  def bind(stmt: PreparedStatement, tuple: Tuple): Boolean
  
  // data of type A is wrapped in AnchoredData to track which tuples to ack
  // when A is finally written to mysql
  var buffer: ListBuffer[Tuple] = _
  
  var ds: DataSource = _
  
  // flush to mysql after specified interval
  override val durationFunctions = Map(interval -> flush())
  
  override def setup(
      stormConfig: java.util.Map[_, _],
      context: TopologyContext,
      outCollector: OutputCollector): Unit = {

    buffer = ListBuffer[Tuple]()
    ds = connectDataSource
  }
  
  override def process(input: Tuple) = buffer += input
  
  def numUnacked = buffer.size
  
  private def flush() = () => { write(buffer); buffer.clear }
  
  /** 
    * - group the map by desired number of upserts per transaction
    * - open prepared statement
    * - bind parameters for prepared statement and create a list of tuples that
    *   should be acked or failed based on success of that batch 
    * - try to upsert.  ack if successful, fail if not
    */
  private def write(dataList: ListBuffer[Tuple]) =
    withPreparedStatement(query, false) { stmt =>
      dataList grouped batchSize foreach { group => 
        val tuples = group flatMap { tryToBind(stmt, _) }
        executeBatch(stmt) match {
          case Right(r) => 
            { stmt.getConnection.commit(); ackAll(tuples) }
          case Left(x: BatchUpdateException) => 
            { stmt.getConnection.commit(); ackAll(tuples) }
          case Left(x: SQLException) => 
            { stmt.getConnection.rollback(); failAll(tuples) }
        }
      }
    }
  
 /** bind data to mysql statement.  on success, return list of tuples for
   * emit anchoring/binding.  on mysql exception, return empty list
   */
  private def tryToBind(stmt: PreparedStatement, tuple: Tuple) =
    try bindAndAdd(stmt, tuple) catch {
      case e: SQLException => { collector.fail(tuple); None }
    }
  
  private def bindAndAdd(stmt: PreparedStatement, tuple: Tuple) = 
    if (bind(stmt, tuple)) { stmt.addBatch(); Some(tuple) }
    else { collector.ack(tuple); None }

  private def ackAll(tuples: Iterable[Tuple]) = tuples foreach { collector.ack }

  private def failAll(tuples: Iterable[Tuple]) = tuples foreach { collector.fail }
}