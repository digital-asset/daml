// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.migrations

import anorm.{BatchSql, NamedParameter}
import com.daml.ledger.participant.state.kvutils.Raw
import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

import java.security.MessageDigest
import java.sql.{Connection, ResultSet}
import scala.collection.compat.immutable.LazyList
import scala.jdk.CollectionConverters._

private[migrations] abstract class V3__Backfill_Key_Hash_State_Table extends BaseJavaMigration {

  private val BatchSize = 1000
  private val TablePrefixPlaceholderName = "table.prefix"

  override def migrate(context: Context): Unit = {
    implicit val conn: Connection = context.getConnection
    val prefix = tablePrefix(context)
    batchUpdatesFor(stateKeys(prefix), prefix).foreach(_.execute())
  }

  private def batchUpdatesFor(
      keys: Iterator[Array[Byte]],
      tablePrefix: String,
  ): Iterator[BatchSql] = {
    val UpdateKeyHashes = s"UPDATE ${tablePrefix}state SET key_hash = {key_hash} WHERE key = {key}"

    keys
      .map { key =>
        List[NamedParameter](
          "key" -> key,
          "key_hash" -> StateKeyHashing.hash(key),
        )
      }
      .to(LazyList)
      .grouped(BatchSize)
      .map { batch =>
        BatchSql(
          UpdateKeyHashes,
          batch.head,
          batch.tail: _*
        )
      }
  }

  private def stateKeys(
      tablePrefix: String
  )(implicit connection: Connection): Iterator[Array[Byte]] = {
    val SelectStateRows = s"SELECT key FROM ${tablePrefix}state"
    val loadStateRows = connection.createStatement()
    loadStateRows.setFetchSize(BatchSize)
    val rows: ResultSet = loadStateRows.executeQuery(SelectStateRows)

    new Iterator[Array[Byte]] {
      override def hasNext: Boolean = rows.next()

      override def next(): Array[Byte] =
        rows.getBytes("key")
    }
  }

  private def tablePrefix(context: Context) =
    context.getConfiguration
      .getPlaceholders()
      .asScala
      .getOrElse(TablePrefixPlaceholderName, throw new RuntimeException("Table prefix missing."))

  object StateKeyHashing {
    def hash(key: Raw.StateKey): Array[Byte] =
      hash(key.bytes.toByteArray)

    def hash(bytes: Array[Byte]): Array[Byte] =
      MessageDigest
        .getInstance("SHA-256")
        .digest(bytes)
  }

}
