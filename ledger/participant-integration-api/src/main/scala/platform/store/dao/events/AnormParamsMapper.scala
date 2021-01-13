package com.daml.platform.store.dao.events

import java.sql.{PreparedStatement, Timestamp}
import java.time.Instant

import anorm.ToStatement

private[events] object AnormParamsMapper {

  class ArrayToStatement[T](arrayType: String) extends ToStatement[Array[T]] {
    override def set(s: PreparedStatement, index: Int, v: Array[T]): Unit = {
      val conn = s.getConnection
      s.setArray(index, conn.createArrayOf(arrayType, v.asInstanceOf[Array[AnyRef]]))
    }
  }

  implicit object ByteArrayArrayToStatement extends ArrayToStatement[Array[Byte]]("BYTEA")

  implicit object CharArrayToStatement extends ArrayToStatement[Array[String]]("VARCHAR")

  implicit object TimestampArrayToStatement extends ArrayToStatement[Array[Timestamp]]("TIMESTAMP")

  // Specific for PostgreSQL parallel unnesting insertions
  implicit object InstantArrayToStatement extends ToStatement[Array[Instant]] {
    override def set(s: PreparedStatement, index: Int, v: Array[Instant]): Unit = {
      val conn = s.getConnection
      val ts = conn.createArrayOf("TIMESTAMP", v.map(java.sql.Timestamp.from))
      s.setArray(index, ts)
    }
  }
}
