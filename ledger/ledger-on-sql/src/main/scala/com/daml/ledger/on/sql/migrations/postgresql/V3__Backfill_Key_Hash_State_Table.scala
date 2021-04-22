// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.migrations.postgresql

import anorm.{BatchSql, NamedParameter}
import com.daml.ledger.on.sql.queries.StateKeyHashing
import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

import java.sql.{Connection, ResultSet}
import scala.collection.compat.immutable.LazyList

private[migrations] class V3__Backfill_Key_Hash_State_Table extends BaseJavaMigration {

  private val BatchSize = 1000

  override def migrate(context: Context): Unit = {
    implicit val conn: Connection = context.getConnection
    batchUpdatesFor(stateKeys).foreach(_.execute())
  }

  private def batchUpdatesFor(keys: Iterator[Array[Byte]]): Iterator[BatchSql] = {
    //TODO: proper prefix and remove key_hash selecting
    val UpdateKeyHashes = "UPDATE ledger_state SET key_hash = {key_hash} WHERE key = {key}"

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
        println(s"BATCH OF SIZE: ${batch.length}")
        BatchSql(
          UpdateKeyHashes,
          batch.head,
          batch.tail: _*
        )
      }
  }

  private def stateKeys(implicit connection: Connection): Iterator[Array[Byte]] = {
    //TODO: proper prefix and remove key_hash selecting
    val SelectStateRows = "SELECT key FROM ledger_state"
    val loadStateRows = connection.createStatement()
    loadStateRows.setFetchSize(BatchSize)
    val rows: ResultSet = loadStateRows.executeQuery(SelectStateRows)

    new Iterator[Array[Byte]] {
      override def hasNext: Boolean = rows.next()

      override def next(): Array[Byte] =
        rows.getBytes("key")
    }
  }
}
