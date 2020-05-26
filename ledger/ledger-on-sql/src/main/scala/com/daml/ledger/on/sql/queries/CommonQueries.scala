// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.sql.Connection

import anorm.SqlParser._
import anorm._
import com.daml.ledger.on.sql.Index
import com.daml.ledger.on.sql.queries.Queries._
import com.daml.ledger.participant.state.kvutils.KVOffset
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

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
          index -> LedgerRecord(KVOffset.fromLong(index), entryId, envelope)
      }.*)
  }

  override final def selectStateValuesByKeys(keys: Seq[Key]): Try[immutable.Seq[Option[Value]]] =
    Try {
      val results =
        SQL"SELECT key, value FROM #$StateTable WHERE key IN ($keys)"
          .fold(Map.newBuilder[Key, Value], ColumnAliaser.empty) { (builder, row) =>
            builder += row("key") -> row("value")
          }
          .fold(exceptions => throw exceptions.head, _.result())
      keys.map(results.get)(breakOut)
    }

  override final def updateState(stateUpdates: Seq[(Key, Value)]): Try[Unit] = Try {
    executeBatchSql(updateStateQuery, stateUpdates.map {
      case (key, value) =>
        Seq[NamedParameter]("key" -> key, "value" -> value)
    })
  }

  protected val updateStateQuery: String
}
