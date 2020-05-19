// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.io.InputStream
import java.sql.{Blob, Connection, PreparedStatement}

import anorm.{
  BatchSql,
  Column,
  MetaDataItem,
  NamedParameter,
  RowParser,
  SqlMappingError,
  SqlParser,
  SqlRequestError,
  ToStatement
}
import com.google.protobuf.ByteString

trait Queries extends ReadQueries with WriteQueries

object Queries {
  val TablePrefix = "ledger"
  val LogTable = s"${TablePrefix}_log"
  val MetaTable = s"${TablePrefix}_meta"
  val StateTable = s"${TablePrefix}_state"

  // By explicitly writing a value to a "table_key" column, we ensure we only ever have one row in
  // the meta table. An attempt to write a second row will result in a key conflict.
  private[queries] val MetaTableKey = 0

  def executeBatchSql(
      query: String,
      params: Iterable[Seq[NamedParameter]],
  )(implicit connection: Connection): Unit = {
    if (params.nonEmpty)
      BatchSql(query, params.head, params.drop(1).toArray: _*).execute()
    ()
  }

  implicit def byteStringToStatement: ToStatement[ByteString] = new ToStatement[ByteString] {
    override def set(s: PreparedStatement, index: Int, v: ByteString): Unit =
      s.setBinaryStream(index, v.newInput(), v.size())
  }

  implicit def columnToByteString: Column[ByteString] =
    Column.nonNull { (value: Any, meta: MetaDataItem) =>
      value match {
        case blob: Blob => Right(ByteString.readFrom(blob.getBinaryStream))
        case byteArray: Array[Byte] => Right(ByteString.copyFrom(byteArray))
        case inputStream: InputStream => Right(ByteString.readFrom(inputStream))
        case _ =>
          Left[SqlRequestError, ByteString](
            SqlMappingError(s"Cannot convert value of column ${meta.column} to ByteString"))
      }
    }

  def getBytes(columnName: String): RowParser[ByteString] =
    SqlParser.get(columnName)(columnToByteString)

}
