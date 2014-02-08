package com.nrelate.wordcount.helper

import java.util.{Map => JMap}

object JavaMapHelper {
  implicit def toJavaMapHelper(m: JMap[_, _]) = new JavaMapHelper(m)
}

class JavaMapHelper(m: JMap[_, _]) {

  /** Mimics Scala Map's {{{apply}}} function
    * 
    * @param k the key used to retrieve a value from the Map
    * @return the value
    * @throws NoSuchElementException if the key does not exist in the Map
    */
  def get(k: AnyRef) = {
    val v = m.get(k)
    if (v == null) throwException(k) else v
  }
  
  def apply[T](k: AnyRef) = get(k).asInstanceOf[T]
  
  private def throwException(k: AnyRef) = 
    throw new NoSuchElementException(s"key $k does not exist in map")
}