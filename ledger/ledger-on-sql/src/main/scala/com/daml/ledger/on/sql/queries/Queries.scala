// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  ToStatement,
}
import com.daml.ledger.participant.state.kvutils.Raw
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

  private val byteStringToStatement: ToStatement[ByteString] =
    (s: PreparedStatement, index: Int, v: ByteString) =>
      s.setBinaryStream(index, v.newInput(), v.size())

  implicit val rawKeyToStatement: ToStatement[Raw.Key] =
    byteStringToStatement.contramap(_.bytes)

  implicit val rawValueToStatement: ToStatement[Raw.Value] =
    byteStringToStatement.contramap(_.bytes)

  private val columnToByteString: Column[ByteString] =
    Column.nonNull { (value: Any, meta: MetaDataItem) =>
      value match {
        case blob: Blob => Right(ByteString.readFrom(blob.getBinaryStream))
        case byteArray: Array[Byte] => Right(ByteString.copyFrom(byteArray))
        case inputStream: InputStream => Right(ByteString.readFrom(inputStream))
        case _ =>
          Left[SqlRequestError, ByteString](
            SqlMappingError(s"Cannot convert value of column ${meta.column} to ByteString")
          )
      }
    }

  implicit val columnToRawKey: Column[Raw.Key] =
    columnToByteString.map(Raw.Key)

  implicit val columnToRawValue: Column[Raw.Value] =
    columnToByteString.map(Raw.Value)

  def rawKey(columnName: String): RowParser[Raw.Key] =
    SqlParser.get(columnName)(columnToRawKey)

  def rawValue(columnName: String): RowParser[Raw.Value] =
    SqlParser.get(columnName)(columnToRawValue)

}
