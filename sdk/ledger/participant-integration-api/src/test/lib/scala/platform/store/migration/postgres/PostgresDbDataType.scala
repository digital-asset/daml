// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.migration.postgres

import java.sql.ResultSet

import com.daml.platform.store.migration.MigrationTestSupport.DbDataType

object PostgresDbDataType {
  case object Integer extends DbDataType {
    override def get(resultSet: ResultSet, index: Int): Any = resultSet.getInt(index)
    override def put(value: Any): String = value.asInstanceOf[Int].toString
  }

  case object BigInt extends DbDataType {
    override def get(resultSet: ResultSet, index: Int): Any = resultSet.getLong(index)
    override def put(value: Any): String = value.asInstanceOf[Long].toString
  }

  case object Str extends DbDataType {
    override def get(resultSet: ResultSet, index: Int): Any = resultSet.getString(index)
    override def put(value: Any): String = s"'${value.asInstanceOf[String]}'"
  }

  case object Bool extends DbDataType {
    override def get(resultSet: ResultSet, index: Int): Any = resultSet.getBoolean(index)
    override def put(value: Any): String = value.asInstanceOf[Boolean].toString
  }

  case object Bytea extends DbDataType {
    override def get(resultSet: ResultSet, index: Int): Any = resultSet.getBytes(index).toVector
    override def put(value: Any): String = value
      .asInstanceOf[Vector[Byte]]
      .map(_.toInt.toHexString)
      .map {
        case hexByte if hexByte.length == 1 => s"0$hexByte"
        case hexByte => hexByte
      }
      .mkString("E'\\\\x", "", "'")
  }

  case object StringArray extends DbDataType {
    override def get(resultSet: ResultSet, index: Int): Any =
      resultSet
        .getArray(index)
        .getArray
        .asInstanceOf[Array[String]]
        .toVector

    override def put(value: Any): String =
      value
        .asInstanceOf[Vector[String]]
        .map(x => s"'$x'")
        .mkString("ARRAY[", ", ", "]::TEXT[]")
  }

  case object IntArray extends DbDataType {
    override def get(resultSet: ResultSet, index: Int): Any =
      resultSet
        .getArray(index)
        .getArray
        .asInstanceOf[Array[java.lang.Integer]]
        .toVector
        .map(_.intValue())

    override def put(value: Any): String =
      value
        .asInstanceOf[Vector[Int]]
        .map(_.toString)
        .mkString("ARRAY[", ", ", "]::INTEGER[]")
  }
}
