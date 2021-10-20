// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.sql.Connection

import anorm.SqlParser._
import anorm._
import com.daml.ledger.on.sql.Index
import com.daml.ledger.on.sql.queries.Queries._
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord
import com.daml.ledger.participant.state.kvutils.{KVOffsetBuilder, Raw}

import scala.collection.compat._
import scala.collection.immutable
import scala.util.Try

abstract class CommonQueries(offsetBuilder: KVOffsetBuilder)(implicit connection: Connection)
    extends Queries {
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
      .as((long("sequence_no") ~ rawLogEntryId("entry_id") ~ rawEnvelope("envelope")).map {
        case index ~ entryId ~ envelope =>
          index -> LedgerRecord(offsetBuilder.of(index), entryId, envelope)
      }.*)
  }

  override final def selectStateValuesByKeys(
      keys: Iterable[Raw.StateKey]
  ): Try[immutable.Seq[Option[Raw.Envelope]]] =
    Try {
      val keyHashes = keys.toSeq.map(StateKeyHashing.hash)
      val results =
        SQL"SELECT key, value FROM #$StateTable WHERE key_hash IN ($keyHashes)"
          .fold(Map.newBuilder[Raw.StateKey, Raw.Envelope], ColumnAliaser.empty) { (builder, row) =>
            builder += row("key")(columnToRawStateKey) -> row("value")(columnToRawEnvelope)
          }
          .fold(exceptions => throw exceptions.head, _.result())
      keys.view.map(results.get).to(immutable.Seq)
    }

  override final def updateState(stateUpdates: Iterable[Raw.StateEntry]): Try[Unit] = Try {
    executeBatchSql(
      updateStateQuery,
      stateUpdates.map { case (key, value) =>
        Seq[NamedParameter](
          "key" -> key,
          "key_hash" -> StateKeyHashing.hash(key),
          "value" -> value,
        )
      },
    )
  }

  protected val updateStateQuery: String
}
