package com.nrelate.wordcount.util

import javax.sql.DataSource
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Statement
import java.sql.SQLException
import java.sql.ResultSet

trait MySQL  {

  var ds: DataSource
  
  def conn: Connection = ds.getConnection
  
  def withConnection[A](autoCommit: Boolean)(f: Connection => A): A = {
    val c = conn
    c.setAutoCommit(autoCommit)
    try f(c) finally c.close
  }
  
  def withPreparedStatement[A](q: String, autoCommit: Boolean)(f: PreparedStatement => A): A = {
    withConnection(autoCommit)(c => {
      val stmt = c.prepareStatement(q)
      try f(stmt) finally stmt.close()
    })
  }
  
  def withStatement[A](autoCommit: Boolean)(f: Statement => A): A = {
    withConnection(autoCommit)(c => {
      val stmt = c.createStatement()
      try f(stmt) finally stmt.close()
    })
  }
  
  def executeUpdate(stmt: PreparedStatement): Either[SQLException, Int] =
    try { Right(stmt.executeUpdate) } catch {
      case e: SQLException =>Left(e)
    }
    
  def executeBatch(stmt: PreparedStatement): Either[SQLException, Array[Int]] =
    try { Right(stmt.executeBatch) } catch {
      case e: SQLException => Left(e)
    }
}