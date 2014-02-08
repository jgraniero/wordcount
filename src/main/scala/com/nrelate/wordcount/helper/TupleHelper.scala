package com.nrelate.wordcount.helper

import scala.language.implicitConversions
import backtype.storm.Constants
import backtype.storm.tuple.Tuple
import scala.util.control.ControlThrowable

/** Pimp yo tuple */
object TupleHelper {
  implicit def toTupleHelper(t: Tuple) = new TupleHelper(t)
}

/** Convenience functions for working with [[backtype.storm.tuple.Tuple]]
  * 
  * @author jgraniero
  */
class TupleHelper(t: Tuple) {
  
  /** Gets string from underlying {{{Tuple}}}
    * 
    * @param index the index of the string in the tuple
    * @return {{{Option}}} containing the {{{String}}} deserialized from the
    *   tuple, or {{{None}}}
    */
  def getStringOpt(index: Int): Option[String] = 
    try Some(t.getString(index)) catch {
      case ex: ControlThrowable => throw ex
      case ex: Throwable => None
  }
  
  def getStringByFieldOpt(field: String): Option[String] = 
    getValueByFieldAs[String](field)
  
  def getValueByFieldAs[A](field: String): Option[A] = 
    try Some(t.getValueByField(field).asInstanceOf[A]) catch {
      case ex: ClassCastException => None
    }
  
  def isTickTuple = 
    t.getSourceComponent == Constants.SYSTEM_COMPONENT_ID &&
    t.getSourceStreamId == Constants.SYSTEM_TICK_STREAM_ID
}