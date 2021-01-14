// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.{PreparedStatement, Timestamp}
import java.time.Instant

import anorm.ToStatement

private[events] object AnormParamsMapper {

  implicit object ByteArrayArrayToStatement extends ToStatement[Array[Array[Byte]]] {
    override def set(s: PreparedStatement, index: Int, v: Array[Array[Byte]]): Unit = {
      val conn = s.getConnection
      val ts = conn.createArrayOf("BYTEA", v.asInstanceOf[Array[AnyRef]])
      s.setArray(index, ts)
    }
  }

  implicit object CharArrayToStatement extends ToStatement[Array[String]] {
    override def set(s: PreparedStatement, index: Int, v: Array[String]): Unit = {
      val conn = s.getConnection
      val ts = conn.createArrayOf("VARCHAR", v.asInstanceOf[Array[AnyRef]])
      s.setArray(index, ts)
    }
  }

  implicit object TimestampArrayToStatement extends ToStatement[Array[Timestamp]] {
    override def set(s: PreparedStatement, index: Int, v: Array[Timestamp]): Unit = {
      val conn = s.getConnection
      val ts = conn.createArrayOf("TIMESTAMP", v.asInstanceOf[Array[AnyRef]])
      s.setArray(index, ts)
    }
  }

  implicit object InstantArrayToStatement extends ToStatement[Array[Instant]] {
    override def set(s: PreparedStatement, index: Int, v: Array[Instant]): Unit = {
      val conn = s.getConnection
      val ts = conn.createArrayOf("TIMESTAMP", v.map(java.sql.Timestamp.from))
      s.setArray(index, ts)
    }
  }
}
