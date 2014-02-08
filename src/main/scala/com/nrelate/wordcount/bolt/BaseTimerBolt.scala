package com.nrelate.wordcount.bolt

import scala.concurrent.duration.FiniteDuration
import com.nrelate.wordcount.helper.TupleHelper._
import backtype.storm.Config
import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Tuple
import java.util.{Map => JMap}
import com.nrelate.wordcount.helper.JavaMapHelper._

/** Abstract bolt that should be extended when functions need to be performed
  * on a fixed interval
  * 
  * This class' finest resolution for time is the second since that is the
  * shortest interval of Storm tick tuples out of the box.
  * 
  * This class will read the value of {{{Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS}}}
  * from the Storm config object.  Note that an action can only be performed
  * on individual ticks, so if a tick comes every 10 seconds and an interval
  * of {{{1 second}}} is passed to {{{BaseTimerBolt}}}, then the function will be
  * executed every 10 seconds
  * 
  * To have functions executed on an interval, add them to the 
  * {{{durationFunctions}}} map as follows:
  * 
  * {{{
  *   def flush = () => // flush code to be performed on interval
  *   def update = () => // update code to be performed on interval
  *   override def durationFunctions = 
  *     Map(5 seconds -> flush, 1 day-> update)
  * }}}
  * 
  * The {{{onInterval}}} function will be triggered after the following number
  * of intervals:
  *   {{{(interval + secondsPerTick - 1) / secondsPerTick}}}
  *   
  * Note that this class overrides 2 of the 3 abstract functions from
  * {{{BaseRichBolt}}}.  As a result, implementing classes must define
  * {{{process}}} rather than {{{execute}}} and {{{setup}}} rather than
  * {{{prepare}}}
  * 
  * This class assigns the {{{OutputCollector}}}, so implementing
  * classes need not do this in the {{{setup}}} function
  * 
  * By default, ticks are not ack'd.
  * 
  * @param ackTicks {{{true}}} if ticks should be ack'd, {{{false}}} if not.
  *   defaults to {{{false}}}
  * 
  * @author jgraniero
  */
abstract class BaseTimerBolt(ackTicks: Boolean = false) extends BaseRichBolt {
  
  var collector: OutputCollector = _
  
  // durationFunctions will be mapped to tickers in the prepare function in case
  // tick frequency config changes
  private var tickers: Iterable[Ticker] = _

  // map of (how often to perform...) -> (...the task to perform)
  def durationFunctions: Map[FiniteDuration, () => Unit]
  
  /** This class overrides {{{prepare}}}, so implementing classes should override
    * {{{setup}}} when extending {{{BaseTimerBolt}}} as they would override
    * {{{prepare}}} when extending {{{BaseRichBolt}}} 
    * 
    * @see [[backtype.storm.task.IBolt#prepare]]
    */
  def setup(conf: JMap[_, _], ctx: TopologyContext, coll: OutputCollector): Unit

  /** This class overrides {{{execute}}}, so implementing classes should override
    * {{{process}}} when extending {{{BaseTimerBolt}}} as they would override
    * {{{execute}}} when extending {{{BaseRichBolt}}}
    * 
    * @see [[backtype.storm.task.IBolt#execute]]
    */
  def process(input: Tuple): Unit 

  override def prepare(
      stormConf: JMap[_, _],
      context: TopologyContext,
      outCollector: OutputCollector): Unit = { 

    collector = outCollector
    
    val secondsPerTick = stormConf[Long](Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS)
    
    // create list of Ticker out of interval -> function map
    tickers = durationFunctions map { case (duration, func) => 
      new Ticker(duration, secondsPerTick, func)
    }

    // implementation specific setup
    setup(stormConf, context, outCollector)
  }
  
  /** This function will swallow any tick tuples, so implementing classes
    * should not expect to get tick tuples in the {{{process(tuple)}}} function
    */
  override def execute(input: Tuple): Unit = 
    if (input.isTickTuple) {
      processTick 
      if (ackTicks) collector.ack(input)
    } else {
      process(input)
    }
  
  private def processTick: Unit = tickers foreach { _.tick } 
}

/** Wrapper to keep track of ticks and execute a function after a certain number
  * of ticks have been seen
  * 
  * @author jgraniero
  */
class Ticker(interval: FiniteDuration, secondsPerTick: Long, f: () => Unit) {

  private var ticks = 0L
  
  private val target = 
    (interval.toSeconds + secondsPerTick - 1) / secondsPerTick

  def tick: Unit = { ticks += 1L; if (ticks >= target) { f(); reset() } }

  private def reset(): Unit = ticks = 0L
}