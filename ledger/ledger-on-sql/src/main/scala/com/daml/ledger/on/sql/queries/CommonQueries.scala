// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.sql.Connection

import anorm.SqlParser._
import anorm._
import com.daml.ledger.on.sql.Index
import com.daml.ledger.on.sql.queries.Queries._
import com.daml.ledger.participant.state.kvutils.OffsetBuilder
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord
import com.daml.ledger.validator.Raw

import scala.collection.{breakOut, immutable}
import scala.util.Try

trait CommonQueries extends Queries {
  protected implicit val connection: Connection

  override final def selectLatestLogEntryId(): Try[Option[Index]] = Try {
    SQL"SELECT MAX(sequence_no) max_sequence_no FROM #$LogTable"
      .as(get[Option[Long]]("max_sequence_no").singleOpt)
      .flatten
  }

  override final def selectFromLog(
      startExclusive: Index,
      endInclusive: Index,
  ): Try[immutable.Seq[(Index, LedgerRecord)]] = Try {
    SQL"SELECT sequence_no, entry_id, envelope FROM #$LogTable WHERE sequence_no > $startExclusive AND sequence_no <= $endInclusive ORDER BY sequence_no"
      .as((long("sequence_no") ~ getBytes("entry_id") ~ getBytes("envelope")).map {
        case index ~ entryId ~ envelope =>
          index -> LedgerRecord(
            OffsetBuilder.fromLong(index),
            Raw.Key(entryId),
            Raw.Value(envelope),
          )
      }.*)
  }

  override final def selectStateValuesByKeys(
      keys: Iterable[Raw.Key],
  ): Try[immutable.Seq[Option[Raw.Value]]] =
    Try {
      val results =
        SQL"SELECT key, value FROM #$StateTable WHERE key IN (${keys.toSeq})"
          .fold(Map.newBuilder[Raw.Key, Raw.Value], ColumnAliaser.empty) { (builder, row) =>
            builder += Raw.Key(row("key")) -> Raw.Value(row("value"))
          }
          .fold(exceptions => throw exceptions.head, _.result())
      keys.map(results.get)(breakOut)
    }

  override final def updateState(stateUpdates: Iterable[Raw.Pair]): Try[Unit] = Try {
    executeBatchSql(updateStateQuery, stateUpdates.map {
      case (key, value) =>
        Seq[NamedParameter]("key" -> key.bytes, "value" -> value.bytes)
    })
  }

  protected val updateStateQuery: String
}
